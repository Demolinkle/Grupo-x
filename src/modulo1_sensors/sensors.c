#include "Common.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, char* argv[]) {
    // Si pasamos un numero por consola lo usa como ID, si no, usa el ID del proceso que da Windows
    DWORD sensor_id = (argc > 1) ? atoi(argv[1]) : GetCurrentProcessId();
    
    // Semilla aleatoria mezclada con el ID para que cada ventana genere numeros diferentes
    srand((unsigned int)time(NULL) ^ sensor_id);

    printf("[SENSOR %lu] Buscando conexion con el Broker Ingestor...\n", (unsigned long)sensor_id);

    // Nos quedamos esperando quietos a que la tuberia del Broker aparezca en el sistema
    if (!WaitNamedPipe(PIPE_NAME, NMPWAIT_WAIT_FOREVER)) {
        printf("[SENSOR %lu] Error: El canal del Broker no esta disponible.\n", (unsigned long)sensor_id);
        return 1;
    }

    // Abrimos la tuberia (Named Pipe) creada por el Broker para conectarnos a ella
    HANDLE hPipe = CreateFile(
        PIPE_NAME,           // Nombre de la tuberia que compartimos
        GENERIC_WRITE,       // Este programa solo va a escribir y mandar datos hacia el Broker
        0,                   // No compartimos este handle con nadie mas
        NULL,                // Seguridad por defecto del sistema operativo
        OPEN_EXISTING,       // Solo se abre si ya el Broker la creo primero
        FILE_ATTRIBUTE_NORMAL,
        NULL
    );

    if (hPipe == INVALID_HANDLE_VALUE) {
        printf("[SENSOR %lu] Error al conectar con el pipe. Codigo: %lu\n", 
               (unsigned long)sensor_id, (unsigned long)GetLastError());
        return 1;
    }

    printf("[SENSOR %lu] Conectado exitosamente. Transmitiendo telemetria...\n", (unsigned long)sensor_id);

    TelemetryEvent event;
    event.sensor_id = sensor_id;
    event.prioridad = rand() % 3; // Le asignamos una prioridad al azar entre 0, 1 o 2
    DWORD bytesWritten = 0;

    // Bucle infinito para mandar datos sin parar
    while (TRUE) {
        // 1. Tomamos el tiempo exacto en microsegundos usando el reloj de alta precision de Windows
        QueryPerformanceCounter(&event.timestamp);

        // 2. Inventamos datos realistas de un Formula 1 usando rangos logicos
        event.velocidad   = 200.0 + (rand() % 120); // Velocidades entre 200 y 320 km/h
        event.rpm         = 8000 + (rand() % 4000);   // Revoluciones entre 8000 y 12000 RPM
        event.temperatura = 75.0 + (rand() % 35);    // Temperaturas entre 75 y 110 grados

        // 3. Mandamos la estructura con los bytes crudos por la tuberia bloqueante hacia el Broker
        BOOL success = WriteFile(hPipe, &event, sizeof(TelemetryEvent), &bytesWritten, NULL);
        
        // Si el Broker se cae o se cierra la tuberia, salimos del bucle para no quedar flotando
        if (!success || bytesWritten == 0) {
            printf("[SENSOR %lu] Conexion perdida con el Broker. Saliendo...\n", (unsigned long)sensor_id);
            break;
        }

        printf("[SENSOR %lu] Evento enviado -> Vel: %.1f km/h | RPM: %.0f\n", 
               (unsigned long)sensor_id, event.velocidad, event.rpm);

        // Esperamos 200 milisegundos antes de mandar la siguiente rafaga de datos
        Sleep(200);
    }

    // PROTOCOLO DE CIERRE LIMPIO EXIGIDO POR LA CATEDRA
    // Cerramos el recurso de la tuberia antes de terminar el programa para no dejar basura en el OS
    CloseHandle(hPipe);
    return 0;
}