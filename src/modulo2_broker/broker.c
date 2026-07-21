#include "Common.h"
#include <stdio.h>

// Recursos globales del sistema operativo para que todo el codigo los pueda usar
HANDLE hMapFile = NULL;
SharedBufferContext* shared_ctx = NULL;

HANDLE hSemEmpty = NULL;
HANDLE hSemFull = NULL;
HANDLE hMutexBuffer = NULL;

HANDLE hEventShutdown = NULL;
BOOL running = TRUE;

// Cada vez que se conecta un coche (sensor), este hilo se encarga de atenderlo a el solo
DWORD WINAPI SensorHandlerThread(LPVOID lpParam) {
    HANDLE hPipe = (HANDLE)lpParam;
    TelemetryEvent event;
    DWORD bytesRead = 0;

    // Sumamos 1 al contador de sensores activos en la memoria compartida de forma segura
    InterlockedIncrement(&shared_ctx->active_sensors);
    printf("[BROKER] Nuevo sensor conectado e hilo asignado.\n");

    while (running) {
        // Nos quedamos esperando pasivamente a que el sensor envie datos por la tuberia
        BOOL success = ReadFile(hPipe, &event, sizeof(TelemetryEvent), &bytesRead, NULL);
        
        // Si el coche se desconecta o da error la lectura, rompemos el bucle para cerrar el hilo
        if (!success || bytesRead == 0) {
            printf("[BROKER] Sensor desconectado o canal cerrado.\n");
            break;
        }

        // --- CONTROL DE INGESTA (PRODUCTOR) ---
        // Si el buffer circular esta lleno (80/80), el hilo se duerme aqui automaticamente
        WaitForSingleObject(hSemEmpty, INFINITE);
        
        // Cerramos la puerta (Mutex) para que ningun otro hilo del Broker intente escribir al mismo tiempo
        WaitForSingleObject(hMutexBuffer, INFINITE);

        // Guardamos los datos del carro en la posicion 'head' del buffer circular
        shared_ctx->data[shared_ctx->head] = event;
        shared_ctx->head = (shared_ctx->head + 1) % BUFFER_SIZE;
        
        // Sumamos 1 a la ocupacion actual para que el monitor lo dibuje en la barra visual
        InterlockedIncrement(&shared_ctx->current_buffer_occupancy);

        // Abrimos la puerta (Mutex) para el siguiente hilo
        ReleaseMutex(hMutexBuffer);
        
        // Avisamos al Dispatcher con el semaforo de que ya pusimos un dato nuevo listo para guardar en log
        ReleaseSemaphore(hSemFull, 1, NULL);
    }

    // El sensor se fue, asi que restamos 1 al contador de conectados
    InterlockedDecrement(&shared_ctx->active_sensors);
    
    // Cerramos la tuberia de este cliente de forma limpia para no dejar basura en el OS
    DisconnectNamedPipe(hPipe);
    CloseHandle(hPipe);
    return 0;
}

int main() {
    printf("[BROKER] Iniciando Servidor de Telemetria...\n");

    // 1. Reservamos el espacio en la memoria RAM para compartir los datos con los otros programas
    hMapFile = CreateFileMapping(
        INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, 0, sizeof(SharedBufferContext), SHM_NAME
    );
    if (hMapFile == NULL) {
        printf("Error al crear File Mapping. Codigo: %lu\n", (unsigned long)GetLastError());
        return 1;
    }

    // 2. Apuntamos nuestro puntero de estructura directamente a ese espacio de memoria creado
    shared_ctx = (SharedBufferContext*)MapViewOfFile(hMapFile, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(SharedBufferContext));
    if (shared_ctx == NULL) {
        printf("Error al mapear vista de memoria. Codigo: %lu\n", (unsigned long)GetLastError());
        CloseHandle(hMapFile);
        return 1;
    }

    // Limpiamos la memoria compartida poniendola toda en cero antes de empezar
    ZeroMemory(shared_ctx, sizeof(SharedBufferContext));

    // 3. Creamos los semaforos y mutex con nombres fijos para que el Dispatcher y Monitor puedan encontrarlos
    hSemEmpty    = CreateSemaphore(NULL, BUFFER_SIZE, BUFFER_SIZE, TEXT("Local\\F1SemEmpty"));
    hSemFull     = CreateSemaphore(NULL, 0, BUFFER_SIZE, TEXT("Local\\F1SemFull"));
    hMutexBuffer = CreateMutex(NULL, FALSE, TEXT("Local\\F1MutexBuffer"));
    hEventShutdown = CreateEvent(NULL, TRUE, FALSE, EVENT_SHUTDOWN);

    printf("[BROKER] Infraestructura central creada. Esperando conexiones de sensores...\n");

    // 4. Bucle infinito para recibir a todos los coches que se quieran conectar
    while (running) {
        // Preparamos una tuberia (Named Pipe) exclusiva en modo de solo lectura de datos
        HANDLE hPipe = CreateNamedPipe(
            PIPE_NAME,
            PIPE_ACCESS_INBOUND, 
            PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT, 
            PIPE_UNLIMITED_INSTANCES,
            sizeof(TelemetryEvent),
            sizeof(TelemetryEvent),
            0, NULL
        );

        if (hPipe == INVALID_HANDLE_VALUE) {
            printf("Error al crear instancia de Named Pipe. Codigo: %lu\n", (unsigned long)GetLastError());
            continue;
        }

        // El programa se queda detenido aqui esperando de forma pasiva a que un sensor abra el canal
        BOOL connected = ConnectNamedPipe(hPipe, NULL) ? TRUE : (GetLastError() == ERROR_PIPE_CONNECTED);

        if (connected) {
            // El coche se conecto con exito. Creamos un hilo exclusivo para el y seguimos escuchando el bucle
            HANDLE hThread = CreateThread(NULL, 0, SensorHandlerThread, (LPVOID)hPipe, 0, NULL);
            if (hThread != NULL) {
                // Cerramos el handle del hilo porque correra libre y no necesitamos controlarlo desde el main
                CloseHandle(hThread); 
            } else {
                CloseHandle(hPipe);
            }
        } else {
            // Si la conexion dio error, cerramos el handle de la tuberia para evitar fugas de recursos
            CloseHandle(hPipe);
        }
    }

    // --- PROTOCOLO DE CIERRE LIMPIO EXIGIDO POR LA CATEDRA ---
    // Nos desconectamos de la memoria y destruimos todos los recursos para no dejar colgado el sistema operativo
    UnmapViewOfFile(shared_ctx);
    CloseHandle(hMapFile);
    CloseHandle(hSemEmpty);
    CloseHandle(hSemFull);
    CloseHandle(hMutexBuffer);
    CloseHandle(hEventShutdown);

    printf("[BROKER] Sistema cerrado ordenadamente.\n");
    return 0;
}