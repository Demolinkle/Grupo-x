#include "Common.h"
#include <stdio.h>

// Variables globales para la gestion de recursos del sistema operativo
HANDLE hMapFile = NULL;
SharedBufferContext* shared_ctx = NULL;

HANDLE hSemEmpty = NULL;
HANDLE hSemFull = NULL;
HANDLE hMutexBuffer = NULL;

HANDLE hEventShutdown = NULL;
BOOL running = TRUE;

// Hilo dedicado a escuchar y leer cada sensor independiente
DWORD WINAPI SensorHandlerThread(LPVOID lpParam) {
    HANDLE hPipe = (HANDLE)lpParam;
    TelemetryEvent event;
    DWORD bytesRead = 0;

    // Registrar un nuevo sensor activo en la memoria compartida
    InterlockedIncrement(&shared_ctx->active_sensors);
    printf("[BROKER] Nuevo sensor conectado e hilo asignado.\n");

    while (running) {
        // Lectura bloqueante desde el Named Pipe
        BOOL success = ReadFile(hPipe, &event, sizeof(TelemetryEvent), &bytesRead, NULL);
        
        // Si el cliente se desconecta o hay error, salimos del bucle
        if (!success || bytesRead == 0) {
            printf("[BROKER] Sensor desconectado o canal cerrado.\n");
            break;
        }

        // PROTOCOLO DE SINCRONIZACION COMPARTIDA (PRODUCTOR)
        // Backpressure pasivo: Si el buffer se llena, el hilo se duerme aqui de forma nativa
        WaitForSingleObject(hSemEmpty, INFINITE);
        
        // Seccion critica: Excluir a otros hilos del Broker para no pisar el indice 'head'
        WaitForSingleObject(hMutexBuffer, INFINITE);

        // Depositar el evento en el buffer circular
        shared_ctx->data[shared_ctx->head] = event;
        shared_ctx->head = (shared_ctx->head + 1) % BUFFER_SIZE;
        
        // Actualizar metricas para el Dashboard
        InterlockedIncrement(&shared_ctx->current_buffer_occupancy);

        // Liberar primitivas de control
        ReleaseMutex(hMutexBuffer);
        ReleaseSemaphore(hSemFull, 1, NULL); // Notificar al Dispatcher que hay un nuevo item
    }

    // Decrementar el conteo de sensores al salir
    InterlockedDecrement(&shared_ctx->active_sensors);
    
    // Cierre limpio del canal de este hilo
    DisconnectNamedPipe(hPipe);
    CloseHandle(hPipe);
    return 0;
}

int main() {
    printf("[BROKER] Iniciando Servidor de Telemetria...\n");

    // 1. Crear el mapeo de memoria compartida (File Mapping)
    hMapFile = CreateFileMapping(
        INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, 0, sizeof(SharedBufferContext), SHM_NAME
    );
    if (hMapFile == NULL) {
        // Se corrige %d a %lu con casteo explicito
        printf("Error al crear File Mapping. Codigo: %lu\n", (unsigned long)GetLastError());
        return 1;
    }

    // 2. Enlazar la vista de memoria al puntero de la estructura
    shared_ctx = (SharedBufferContext*)MapViewOfFile(hMapFile, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(SharedBufferContext));
    if (shared_ctx == NULL) {
        // Se corrige %d a %lu con casteo explicito
        printf("Error al mapear vista de memoria. Codigo: %lu\n", (unsigned long)GetLastError());
        CloseHandle(hMapFile);
        return 1;
    }

    // Inicializar el espacio de memoria compartida en cero
    ZeroMemory(shared_ctx, sizeof(SharedBufferContext));

    // 3. Crear los objetos de sincronizacion global
    hSemEmpty    = CreateSemaphore(NULL, BUFFER_SIZE, BUFFER_SIZE, NULL);
    hSemFull     = CreateSemaphore(NULL, 0, BUFFER_SIZE, NULL);
    hMutexBuffer = CreateMutex(NULL, FALSE, NULL);
    hEventShutdown = CreateEvent(NULL, TRUE, FALSE, EVENT_SHUTDOWN);

    printf("[BROKER] Infraestructura central creada. Esperando conexiones de sensores...\n");

    // 4. Bucle principal de escucha de Named Pipes
    while (running) {
        // Crear una instancia del pipe para el proximo cliente
        HANDLE hPipe = CreateNamedPipe(
            PIPE_NAME,
            PIPE_ACCESS_INBOUND, // El Broker solo lee datos del sensor
            PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT, // Modo bloqueante pasivo
            PIPE_UNLIMITED_INSTANCES,
            sizeof(TelemetryEvent),
            sizeof(TelemetryEvent),
            0, NULL
        );

        if (hPipe == INVALID_HANDLE_VALUE) {
            // Se corrige %d a %lu con casteo explicito
            printf("Error al crear instancia de Named Pipe. Codigo: %lu\n", (unsigned long)GetLastError());
            continue;
        }

        // Esperar a que un proceso sensor ejecute su conexion
        BOOL connected = ConnectNamedPipe(hPipe, NULL) ? TRUE : (GetLastError() == ERROR_PIPE_CONNECTED);

        if (connected) {
            // Delegar la lectura a un hilo receptor dedicado (Multihilo dinamico)
            HANDLE hThread = CreateThread(NULL, 0, SensorHandlerThread, (LPVOID)hPipe, 0, NULL);
            if (hThread != NULL) {
                CloseHandle(hThread); // Cerramos el handle del hilo porque no necesitamos rastrearlo aqui
            } else {
                CloseHandle(hPipe);
            }
        } else {
            // Si la conexion fallo, cerramos el handle inmediatamente para evitar fugas
            CloseHandle(hPipe);
        }
    }

    // PROTOCOLO DE CIERRE LIMPIO DE HANDLES
    UnmapViewOfFile(shared_ctx);
    CloseHandle(hMapFile);
    CloseHandle(hSemEmpty);
    CloseHandle(hSemFull);
    CloseHandle(hMutexBuffer);
    CloseHandle(hEventShutdown);

    printf("[BROKER] Sistema cerrado ordenadamente.\n");
    return 0;
}