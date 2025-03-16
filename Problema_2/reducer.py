#!/usr/bin/python3

"""
Descripción
Este script es un reducer para Hadoop Streaming que calcula el promedio de los puntajes asociados a cada fuente (source).

Procesa pares clave-valor tabulados (source \t score) desde la entrada estándar y calcula:

La suma acumulada de los puntajes por fuente.

El número de elementos para calcular la media.

Finalmente, emite los resultados en formato tabulado (source \t mean).
"""

import sys

# Función principal para el reducer
def reducer():
    # Diccionario para almacenar los datos agregados por fuente
    data = {}
    
    # Leer línea por línea desde la entrada estándar (stdin)
    for line in sys.stdin:
        # Dividir la línea en partes usando el tabulador como delimitador
        parts = line.strip().split("\t")
        if len(parts) != 2:
            # Ignorar líneas mal formateadas
            continue
        source, score = parts
        if score == "NULL":
            # Ignorar valores "NULL"
            continue
        try:
            # Intentar convertir el puntaje a un número flotante
            score_val = float(score)
        except ValueError:
            # Ignorar valores que no se puedan convertir
            continue
        
        # Agregar o actualizar los datos agregados para la fuente
        if source not in data:
            # Inicializar suma y contador para una fuente nueva
            data[source] = {'sum': score_val, 'count': 1}
        else:
            # Actualizar suma y contador para una fuente existente
            data[source]['sum'] += score_val
            data[source]['count'] += 1

    # Calcular y emitir la media por cada fuente
    for source, values in data.items():
        if values['count'] > 0:
            # Calcular el promedio de los puntajes
            mean_value = values['sum'] / values['count']
            print(f"{source}\t{mean_value}")

# Punto de entrada del script
if __name__ == "__main__":
    reducer()
