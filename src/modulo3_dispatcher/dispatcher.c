#include "Common.h"
#include <stdio.h>
#include <stdlib.h>

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

// Hilo trabajador (Worker) del Pool
DWORD WINAPI WorkerThread(LPVOID lpParam) {
    DWORD bytesTransferred = 0;
    ULONG_PTR completionKey = 0;
    LPOVERLAPPED pOverlapped = NULL;

    while (running) {
        // Extraer trabajo de la cola IOCP de forma eficiente (bloqueo pasivo)
        BOOL res = GetQueuedCompletionStatus(
            hIOCP, &bytesTransferred, &completionKey, &pOverlapped, INFINITE
        );

        // Si la clave de completitud es la señal de parada, salimos
        if (completionKey == (ULONG_PTR)-1) {
            break;
        }

        if (res && pOverlapped != NULL) {
            TelemetryEvent* event = (TelemetryEvent*)pOverlapped;

            // Simular carga de computo de procesamiento (ej. analisis de telemetria)
            Sleep(50);

            // Preparar estructura OVERLAPPED para escribir de forma segura en la cola del archivo
            OVERLAPPED ovWrite = {0};
            ovWrite.Offset = 0xFFFFFFFF;     // Constante de Win32 para escribir al final del archivo (Append)
            ovWrite.OffsetHigh = 0xFFFFFFFF;

            // Bloquear de forma exclusiva la region del archivo para que ningun otro hilo se pise
            LockFileEx(hLogFile, LOCKFILE_EXCLUSIVE_LOCK, 0, sizeof(TelemetryEvent), 0, &ovWrite);

            DWORD bytesWritten = 0;
            WriteFile(hLogFile, event, sizeof(TelemetryEvent), &bytesWritten, &ovWrite);

            // Liberar el bloqueo del archivo inmediatamente despues de escribir
            UnlockFileEx(hLogFile, 0, sizeof(TelemetryEvent), 0, &ovWrite);

            // Actualizar contador historico global en la memoria compartida
            InterlockedIncrement(&shared_ctx->total_processed_events);

            // Liberar la memoria asignada dinamicamente por el Dispatcher
            free(event);
        }
    }
    return 0;
}

int main() {
    printf("[DISPATCHER] Iniciando Subsistema Distribuidor...\n");

    // 1. Abrir la memoria compartida creada por el Broker
    hMapFile = OpenFileMapping(FILE_MAP_ALL_ACCESS, FALSE, SHM_NAME);
    if (hMapFile == NULL) {
        printf("Error: El Broker central no esta iniciado.\n");
        return 1;
    }

    shared_ctx = (SharedBufferContext*)MapViewOfFile(hMapFile, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(SharedBufferContext));
    
    // 2. Abrir primitivas de sincronizacion existentes
    hSemEmpty    = OpenSemaphore(SEMAPHORE_ALL_ACCESS, FALSE, TEXT("Local\\F1SemEmpty")); // Se asume creado por Broker
    hSemFull     = OpenSemaphore(SEMAPHORE_ALL_ACCESS, FALSE, TEXT("Local\\F1SemFull"));
    hMutexBuffer = OpenMutex(MUTEX_ALL_ACCESS, FALSE, TEXT("Local\\F1MutexBuffer"));
    hEventShutdown = OpenEvent(EVENT_ALL_ACCESS, FALSE, EVENT_SHUTDOWN);

    // Si los Named Objects locales del Broker no tienen nombres fijos, se abren usando punteros locales
    // Nota: Para simplificar el desacoplamiento de procesos independientes, los abrimos de manera global
    hSemEmpty    = OpenSemaphore(SEMAPHORE_ALL_ACCESS, FALSE, NULL); 
    hSemFull     = OpenSemaphore(SEMAPHORE_ALL_ACCESS, FALSE, NULL);
    hMutexBuffer = OpenMutex(MUTEX_ALL_ACCESS, FALSE, NULL);

    // Alternativa robusta: Al ser procesos independientes, el enunciado exige abrirlos por jerarquia.
    // Como compartimos la misma memoria, podemos re-crear/abrir los objetos usando nombres compartidos indirectamente.
    // Para esta plantilla, asumimos que el Broker y el Dispatcher coordinan mediante los semaforos anonimos o mapeados.
    // Como son procesos separados, necesitamos pasar los Handles o crearlos con nombres fijos. 
    // Vamos a usar nombres fijos locales para garantizar la apertura entre procesos separados:
    hSemEmpty    = CreateSemaphore(NULL, BUFFER_SIZE, BUFFER_SIZE, TEXT("Local\\F1SemEmpty"));
    hSemFull     = CreateSemaphore(NULL, 0, BUFFER_SIZE, TEXT("Local\\F1SemFull"));
    hMutexBuffer = CreateMutex(NULL, FALSE, TEXT("Local\\F1MutexBuffer"));

    // 3. Crear el archivo de log indexado concurrentemente
    hLogFile = CreateFile(
        TEXT("telemetry_output.log"), GENERIC_WRITE, 
        FILE_SHARE_WRITE | FILE_SHARE_READ, NULL, 
        OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL
    );

    // 4. Inicializar el Puerto de Finalizacion de E/S (IOCP) para la cola de hilos
    hIOCP = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, MAX_WORKERS);

    // 5. Levantar el Pool de Hilos Trabajadores (Workers)
    for (int i = 0; i < MAX_WORKERS; i++) {
        hWorkers[i] = CreateThread(NULL, 0, WorkerThread, NULL, 0, NULL);
    }

    printf("[DISPATCHER] Pool de %d Workers listo y escuchando cola IOCP...\n", MAX_WORKERS);

    // Bucle de consumo del Buffer Circular
    while (running) {
        // Verificar si se activo el evento de apagado global
        if (hEventShutdown && WaitForSingleObject(hEventShutdown, 0) == WAIT_OBJECT_0) {
            break;
        }

        // Esperar pasivamente a que el Broker deposite algo en el buffer compartido
        DWORD waitRes = WaitForSingleObject(hSemFull, 100);
        if (waitRes == WAIT_TIMEOUT) continue;
        if (waitRes != WAIT_OBJECT_0) break;

        // Reservar dinamicamente espacio para enviar el evento al pool
        TelemetryEvent* isolatedEvent = (TelemetryEvent*)malloc(sizeof(TelemetryEvent));
        
        // Seccion critica de lectura del buffer compartido
        WaitForSingleObject(hMutexBuffer, INFINITE);

        *isolatedEvent = shared_ctx->data[shared_ctx->tail];
        shared_ctx->tail = (shared_ctx->tail + 1) % BUFFER_SIZE;
        
        // Decrementar ocupacion actual para el Dashboard
        InterlockedDecrement(&shared_ctx->current_buffer_occupancy);

        ReleaseMutex(hMutexBuffer);
        ReleaseSemaphore(hSemEmpty, 1, NULL); // Liberar espacio para la ingesta del Broker (Backpressure)

        // Enviar el puntero del evento al Pool de Workers mediante el IOCP sin bloquear el hilo principal
        PostQueuedCompletionStatus(hIOCP, sizeof(TelemetryEvent), 0, (LPOVERLAPPED)isolatedEvent);
    }

    // --- PROTOCOLO DE APAGADO LIMPIO ---
    // Enviar señal de terminacion a cada hilo del pool
    for (int i = 0; i < MAX_WORKERS; i++) {
        PostQueuedCompletionStatus(hIOCP, 0, (ULONG_PTR)-1, NULL);
    }

    // Esperar a que todos los workers finalicen sus tareas pendientes ordenadamente
    WaitForMultipleObjects(MAX_WORKERS, hWorkers, TRUE, INFINITE);

    for (int i = 0; i < MAX_WORKERS; i++) {
        CloseHandle(hWorkers[i]);
    }

    CloseHandle(hIOCP);
    CloseHandle(hLogFile);
    UnmapViewOfFile(shared_ctx);
    CloseHandle(hMapFile);

    printf("[DISPATCHER] Pool de Workers cerrado limpiamente.\n");
    return 0;
}