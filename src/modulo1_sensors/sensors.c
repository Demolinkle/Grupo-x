#include "Common.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, char* argv[]) {
    // Si se pasa un argumento por consola se usa como ID, si no, se usa el ID del proceso actual
    DWORD sensor_id = (argc > 1) ? atoi(argv[1]) : GetCurrentProcessId();
    
    // Semilla para generar datos aleatorios unicos por cada sensor instanciado
    srand((unsigned int)time(NULL) ^ sensor_id);

    printf("[SENSOR %d] Buscando conexion con el Broker Ingestor...\n", sensor_id);

    // Esperar de forma pasiva a que el pipe este disponible en el sistema operativo
    if (!WaitNamedPipe(PIPE_NAME, NMPWAIT_WAIT_FOREVER)) {
        printf("[SENSOR %d] Error: El canal del Broker no esta disponible.\n", sensor_id);
        return 1;
    }

    // Conectarse al Named Pipe creado por el Broker
    HANDLE hPipe = CreateFile(
        PIPE_NAME,           // Nombre de la tuberia
        GENERIC_WRITE,       // El sensor solo escribe datos hacia el Broker
        0,                   // No se comparte el acceso de este handle
        NULL,                // Atributos de seguridad por defecto
        OPEN_EXISTING,       // Abrir solo si ya existe (creado por el Broker)
        FILE_ATTRIBUTE_NORMAL,
        NULL
    );

    if (hPipe == INVALID_HANDLE_VALUE) {
        printf("[SENSOR %d] Error al conectar con el pipe. Codigo: %d\n", sensor_id, GetLastError());
        return 1;
    }

    printf("[SENSOR %d] Conectado exitosamente. Transmitiendo telemetria...\n", sensor_id);

    TelemetryEvent event;
    event.sensor_id = sensor_id;
    event.prioridad = rand() % 3; // Asignar una prioridad aleatoria (0, 1 o 2)
    DWORD bytesWritten = 0;

    // Bucle de transmision continua
    while (TRUE) {
        // 1. Obtener la marca de tiempo de alta resolucion del sistema operativo
        QueryPerformanceCounter(&event.timestamp);

        // 2. Simular payload de métricas de un Formula 1
        event.velocidad   = 200.0 + (rand() % 120); // Entre 200 y 320 km/h
        event.rpm         = 8000 + (rand() % 4000);   // Entre 8000 y 12000 RPM
        event.temperatura = 75.0 + (rand() % 35);    // Entre 75 y 110 grados Celsius

        // 3. Enviar estructura empaquetada a traves del pipe bloqueante
        BOOL success = WriteFile(hPipe, &event, sizeof(TelemetryEvent), &bytesWritten, NULL);
        
        // Si el Broker se cierra o se rompe la conexion, salimos del bucle
        if (!success || bytesWritten == 0) {
            printf("[SENSOR %d] Conexion perdida con el Broker. Saliendo...\n", sensor_id);
            break;
        }

        printf("[SENSOR %d] Evento enviado -> Vel: %.1f km/h | RPM: %.0f\n", 
               sensor_id, event.velocidad, event.rpm);

        // Simular tasa de refresco del sensor (envia datos cada 200 milisegundos)
        Sleep(200);
    }

    // Cierre ordenado del handle al terminar
    CloseHandle(hPipe);
    return 0;
}