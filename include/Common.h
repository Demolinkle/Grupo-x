#ifndef COMMON_H
#define COMMON_H

#include <windows.h>

// --- CONFIGURACION GLOBAL DEL SISTEMA ---
#define BUFFER_SIZE    80    // Capacidad maxima del buffer circular
#define MAX_WORKERS    4     // Numero de hilos trabajadores concurrentes

// Nombres unicos de recursos del Nucleo de Windows (Namespaces locales)
#define PIPE_NAME           TEXT("\\\\.\\pipe\\F1TelemetryPipe")
#define SHM_NAME            TEXT("Local\\F1TelemetrySharedMemory")
#define EVENT_SHUTDOWN      TEXT("Local\\F1EventShutdown")
#define EVENT_DEBUG_TOGGLE  TEXT("Local\\F1EventDebugToggle")

// --- ESTRUCTURAS DE DATOS ---

// Estructura empaquetada que viaja por el Named Pipe y se aloja en Memoria Compartida
typedef struct {
    DWORD sensor_id;           // ID unico del proceso sensor (Modulo 1)
    LARGE_INTEGER timestamp;   // Marca de tiempo de alta resolucion (QueryPerformanceCounter)
    double velocidad;          // Payload simulacion: Velocidad en km/h
    double rpm;                // Payload simulacion: Revoluciones por minuto
    double temperatura;        // Payload simulacion: Temperatura del componente
    LONG prioridad;            // Analizada por el Dispatcher (0: Alta, 1: Media, 2: Baja)
} TelemetryEvent;

// Layout exacto alojado dentro del File Mapping de Windows
typedef struct {
    TelemetryEvent data[BUFFER_SIZE]; // El arreglo circular fisico
    LONG head;                        // Indice de escritura (Modificado por hilos del Broker)
    LONG tail;                        // Indice de lectura (Modificado por el Dispatcher)
    
    // Metricas compartidas para el Dashboard del Modulo 4
    LONG active_sensors;              // Conteo de sensores concurrentes en tiempo real
    LONG total_processed_events;      // Historico total desde el encendido
    LONG current_buffer_occupancy;    // Tasa de ocupacion actual (0 a BUFFER_SIZE)
} SharedBufferContext;

#endif // COMMON_H