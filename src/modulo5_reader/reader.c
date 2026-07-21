#include "Common.h"
#include <stdio.h>

int main() {
    FILE* file = fopen("telemetry_output.log", "rb");
    if (!file) {
        printf("No se pudo abrir el archivo de log.\n");
        return 1;
    }

    TelemetryEvent event;
    int count = 0;

    // Lee la estructura binaria bloque por bloque
    while (fread(&event, sizeof(TelemetryEvent), 1, file) == 1) {
        count++;
        printf("[%d] Sensor ID: %lu | Vel: %.1f km/h | RPM: %.0f | Temp: %.1f C | Prio: %ld\n",
            count, 
            (unsigned long)event.sensor_id, 
            event.velocidad, 
            event.rpm, 
            event.temperatura, 
            event.prioridad);
    }

    fclose(file);
    printf("\nTotal de eventos leidos: %d\n", count);
    getchar();
    return 0;
}