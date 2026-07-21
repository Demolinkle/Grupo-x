#include "Common.h"
#include <stdio.h>
#include <stdlib.h>

int main() {
    // 1. Intentar abrir el mapeo de memoria existente creado por el Broker
    HANDLE hMapFile = OpenFileMapping(FILE_MAP_READ, FALSE, SHM_NAME);
    if (hMapFile == NULL) {
        printf("[MONITOR] Error: El Broker central no esta iniciado. Enciendalo primero.\n");
        return 1;
    }

    // 2. Enlazar la vista de memoria en modo solo lectura
    SharedBufferContext* shared_ctx = (SharedBufferContext*)MapViewOfFile(
        hMapFile, FILE_MAP_READ, 0, 0, sizeof(SharedBufferContext)
    );
    
    if (shared_ctx == NULL) {
        printf("[MONITOR] Error al mapear la vista de memoria. Codigo: %lu\n", (unsigned long)GetLastError());
        CloseHandle(hMapFile);
        return 1;
    }

    // 3. Crear o abrir el evento de Windows para el protocolo de apagado global
    HANDLE hEventShutdown = CreateEvent(NULL, TRUE, FALSE, EVENT_SHUTDOWN);

    printf("\n");
    printf("   DASHBOARD DE TELEMETRIA FORMULA 1 - WIN32        \n");
    printf("\n");
    printf(" Monitoreando infraestructura en tiempo real...\n");
    printf(" Presione la tecla 'Q' para iniciar el apagado limpio global.\n\n");

    // Bucle de actualizacion del Dashboard
    while (TRUE) {
        // Limpieza rapida de pantalla en consola Windows
        system("cls");

        // Leer datos directamente de la memoria compartida de forma segura
        LONG sensores_activos = shared_ctx->active_sensors;
        LONG total_procesados = shared_ctx->total_processed_events;
        LONG ocupacion_actual = shared_ctx->current_buffer_occupancy;

        printf("\n");
        printf(" ESTADO DEL HARDWARE Y SOFTWARE\n");
        printf("\n");
        printf(" -> Sensores Conectados Concurrentes : %ld\n", sensores_activos);
        printf(" -> Total Eventos Persistidos en Log : %ld\n", total_procesados);
        printf(" -> Ocupacion del Buffer Circular    : %ld / %d\n", ocupacion_actual, BUFFER_SIZE);

        // Renderizar una barra visual de la tasa de ocupacion del buffer
        printf(" Tasa de Ocupacion: [");
        for (int i = 0; i < BUFFER_SIZE; i++) {
            if (i < ocupacion_actual) {
                printf("#"); // Espacio ocupado por un evento sin procesar
            } else {
                printf("."); // Espacio libre vacio
            }
        }
        printf("]\n");
        printf("\n");

        // Validar de forma asincrona si el usuario presiona la tecla 'Q' en la consola
        if (GetAsyncKeyState('Q') & 0x8000) {
            printf("\n[MONITOR] Tecla 'Q' detectada. Activando Evento de Apagado Global...\n");
            
            if (hEventShutdown != NULL) {
                SetEvent(hEventShutdown); // Transmitir la señal nativa de Windows a los demas procesos
            }
            break;
        }

        // Frecuencia de muestreo visual del Dashboard (Cada 300 milisegundos)
        Sleep(300);
    }

    // PROTOCOLO DE CIERRE LIMPIO EXIGIDO
    UnmapViewOfFile(shared_ctx);
    CloseHandle(hMapFile);
    if (hEventShutdown != NULL) {
        CloseHandle(hEventShutdown);
    }

    printf("[MONITOR] Dashboard cerrado y Handles liberados.\n");
    return 0;
}