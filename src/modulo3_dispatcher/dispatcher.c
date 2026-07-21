#include "Common.h"
#include <stdio.h>
#include <stdlib.h>

// Variables globales para manejar la memoria, los hilos y el archivo de texto
HANDLE hMapFile = NULL;
SharedBufferContext* shared_ctx = NULL;

HANDLE hSemEmpty = NULL;
HANDLE hSemFull = NULL;
HANDLE hMutexBuffer = NULL;
HANDLE hEventShutdown = NULL;

HANDLE hIOCP = NULL;
HANDLE hLogFile = NULL;
HANDLE hWorkers[MAX_WORKERS];
BOOL running = TRUE;

// Este es el codigo que ejecuta cada hilo trabajador del Pool en paralelo
DWORD WINAPI WorkerThread(LPVOID lpParam) {
    DWORD bytesTransferred = 0;
    ULONG_PTR completionKey = 0;
    LPOVERLAPPED pOverlapped = NULL;

    while (running) {
        // Sacamos un trabajo de la cola IOCP. Si no hay nada, el hilo se queda dormido sin consumir CPU
        BOOL res = GetQueuedCompletionStatus(
            hIOCP, &bytesTransferred, &completionKey, &pOverlapped, INFINITE
        );

        // Si la clave recibida es un -1, significa que nos mandaron la señal de apagar el hilo
        if (completionKey == (ULONG_PTR)-1) {
            break;
        }

        if (res && pOverlapped != NULL) {
            TelemetryEvent* event = (TelemetryEvent*)pOverlapped;

            // Simulamos el tiempo que tarda el hilo analizando y procesando los datos recibidos
            Sleep(50);

            // Preparamos la estructura para decirle a Windows que escriba al final del archivo
            OVERLAPPED ovWrite = {0};
            ovWrite.Offset = 0xFFFFFFFF;     
            ovWrite.OffsetHigh = 0xFFFFFFFF;

            // Bloqueamos el archivo en este pedazo para que ningun otro hilo intente escribir a la vez
            LockFileEx(hLogFile, LOCKFILE_EXCLUSIVE_LOCK, 0, sizeof(TelemetryEvent), 0, &ovWrite);

            DWORD bytesWritten = 0;
            WriteFile(hLogFile, event, sizeof(TelemetryEvent), &bytesWritten, &ovWrite);

            // Soltamos el bloqueo del archivo inmediatamente para que los demas hilos puedan usarlo
            UnlockFileEx(hLogFile, 0, sizeof(TelemetryEvent), 0, &ovWrite);

            // Sumamos 1 de forma segura al contador total de datos guardados con exito en el log
            InterlockedIncrement(&shared_ctx->total_processed_events);

            // Liberamos la memoria temporal que habiamos reservado para este evento especifico
            free(event);
        }
    }
    return 0;
}

int main() {
    printf("[DISPATCHER] Iniciando Subsistema Distribuidor...\n");

    // 1. Buscamos la memoria compartida que el Broker debio crear primero en la RAM
    hMapFile = OpenFileMapping(FILE_MAP_ALL_ACCESS, FALSE, SHM_NAME);
    if (hMapFile == NULL) {
        printf("Error: El Broker central no esta iniciado. Codigo: %lu\n", (unsigned long)GetLastError());
        return 1;
    }

    // Enlazamos nuestro puntero a esa region de memoria compartida para poder leer el buffer
    shared_ctx = (SharedBufferContext*)MapViewOfFile(hMapFile, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(SharedBufferContext));
    if (shared_ctx == NULL) {
        printf("Error al mapear memoria compartida. Codigo: %lu\n", (unsigned long)GetLastError());
        CloseHandle(hMapFile);
        return 1;
    }
    
    // 2. Abrimos los mismos semaforos y mutex del Broker usando exactamente sus mismos nombres
    hSemEmpty    = CreateSemaphore(NULL, BUFFER_SIZE, BUFFER_SIZE, TEXT("Local\\F1SemEmpty"));
    hSemFull     = CreateSemaphore(NULL, 0, BUFFER_SIZE, TEXT("Local\\F1SemFull"));
    hMutexBuffer = CreateMutex(NULL, FALSE, TEXT("Local\\F1MutexBuffer"));
    hEventShutdown = CreateEvent(NULL, TRUE, FALSE, EVENT_SHUTDOWN);

    // 3. Abrimos o creamos el archivo log donde se van a guardar todos los datos de los carros
    hLogFile = CreateFile(
        TEXT("telemetry_output.log"), GENERIC_WRITE, 
        FILE_SHARE_WRITE | FILE_SHARE_READ, NULL, 
        OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL
    );

    if (hLogFile == INVALID_HANDLE_VALUE) {
        printf("Error al crear el archivo de log. Codigo: %lu\n", (unsigned long)GetLastError());
        UnmapViewOfFile(shared_ctx);
        CloseHandle(hMapFile);
        return 1;
    }

    // 4. Creamos la cola IOCP que servira para repartir el trabajo de forma ordenada a los hilos
    hIOCP = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, MAX_WORKERS);

    // 5. Creamos y encendemos el grupo de hilos trabajadores (Pool)
    for (int i = 0; i < MAX_WORKERS; i++) {
        hWorkers[i] = CreateThread(NULL, 0, WorkerThread, NULL, 0, NULL);
    }

    printf("[DISPATCHER] Pool de %d Workers listo y escuchando cola IOCP...\n", MAX_WORKERS);

    // Bucle principal para sacar la informacion del buffer circular
    while (running) {
        // Si alguien presiono la Q en el monitor, este evento se activa y cerramos el programa
        if (hEventShutdown && WaitForSingleObject(hEventShutdown, 0) == WAIT_OBJECT_0) {
            break;
        }

        // Nos quedamos esperando a que el semaforo avise que el Broker dejo un dato en el buffer
        DWORD waitRes = WaitForSingleObject(hSemFull, 100);
        if (waitRes == WAIT_TIMEOUT) continue;
        if (waitRes != WAIT_OBJECT_0) break;

        // Reservamos un bloque de memoria limpio para enviarselo al hilo de la cola IOCP
        TelemetryEvent* isolatedEvent = (TelemetryEvent*)malloc(sizeof(TelemetryEvent));
        if (isolatedEvent == NULL) continue;
        
        // Entramos a la seccion critica cerrando la puerta (Mutex) para leer del buffer sin interferencias
        WaitForSingleObject(hMutexBuffer, INFINITE);

        // Copiamos el dato de la posicion 'tail' y avanzamos el indice de forma circular
        *isolatedEvent = shared_ctx->data[shared_ctx->tail];
        shared_ctx->tail = (shared_ctx->tail + 1) % BUFFER_SIZE;
        
        // Restamos 1 a la ocupacion porque acabamos de sacar un elemento del buffer
        InterlockedDecrement(&shared_ctx->current_buffer_occupancy);

        // Abrimos la puerta (Mutex) y aumentamos el semaforo vacio para avisar que hay un espacio libre
        ReleaseMutex(hMutexBuffer);
        ReleaseSemaphore(hSemEmpty, 1, NULL); 

        // Metemos el dato en la cola IOCP para que cualquiera de los hilos libres lo agarre y lo guarde
        PostQueuedCompletionStatus(hIOCP, sizeof(TelemetryEvent), 0, (LPOVERLAPPED)isolatedEvent);
    }

    // PROTOCOLO DE CIERRE LIMPIO EXIGIDO POR LA CATEDRA
    // Le mandamos a cada hilo del pool la clave de parada (-1) para que sepan que deben cerrarse
    for (int i = 0; i < MAX_WORKERS; i++) {
        PostQueuedCompletionStatus(hIOCP, 0, (ULONG_PTR)-1, NULL);
    }

    // Esperamos pacientemente en el main a que todos los hilos del pool terminen lo que estaban haciendo
    WaitForMultipleObjects(MAX_WORKERS, hWorkers, TRUE, INFINITE);

    // Destruimos y cerramos todos los handles abiertos para liberar por completo los recursos en Windows
    for (int i = 0; i < MAX_WORKERS; i++) {
        CloseHandle(hWorkers[i]);
    }

    CloseHandle(hIOCP);
    CloseHandle(hLogFile);
    UnmapViewOfFile(shared_ctx);
    CloseHandle(hMapFile);
    CloseHandle(hSemEmpty);
    CloseHandle(hSemFull);
    CloseHandle(hMutexBuffer);
    CloseHandle(hEventShutdown);

    printf("[DISPATCHER] Pool de Workers cerrado limpiamente.\n");
    return 0;
}