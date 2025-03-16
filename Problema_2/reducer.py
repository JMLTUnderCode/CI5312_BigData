#!/usr/bin/python3

"""
Descripción
Este script es un reducer para Hadoop Streaming que calcula el promedio de los puntajes asociados a cada fuente (source).

Procesa pares clave-valor tabulados (source \t score) desde la entrada estándar y calcula:
- La suma acumulada de los puntajes por fuente.
- El número de elementos para calcular la media.
- Finalmente, emite los resultados con formato tabulado y detalles descriptivos.
"""

import sys

def reducer():
    # Diccionario para almacenar datos agregados por fuente
    data = {}

    # Leer y procesar la entrada estándar (stdin)
    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue  # Ignorar líneas mal formateadas

        source, score = parts
        if score == "NULL":
            continue  # Ignorar valores "NULL"

        try:
            score_val = float(score)  # Convertir el puntaje a un número flotante
        except ValueError:
            continue  # Ignorar valores no numéricos

        # Agregar o actualizar los datos en el diccionario
        if source not in data:
            data[source] = {'sum': score_val, 'count': 1}
        else:
            data[source]['sum'] += score_val
            data[source]['count'] += 1

    # Emitir resultados calculados con un formato más claro
    print("Fuente\tPromedio de Puntuaciones")
    print("-------------------------------")
    for source, values in sorted(data.items()):
        mean_value = values['sum'] / values['count']
        print(f"{source}\t{mean_value:.2f}")

if __name__ == "__main__":
    reducer()
