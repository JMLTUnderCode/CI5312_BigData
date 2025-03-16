#!/usr/bin/python3

"""
Reducer para Hadoop Streaming

Este script es un componente del modelo MapReduce. Recibe pares clave-valor
`source \t score` desde la entrada estándar, calcula el promedio de puntuaciones
por cada fuente (`source`) y emite los resultados en un formato tabulado.

Funcionalidades:
    - Calcula:
        - Suma acumulada de puntajes por fuente.
        - Total de elementos (conteo) por fuente.
        - Promedio de puntajes por fuente.
    - Ignora valores "NULL" o no numéricos en el campo de puntaje.

Entrada esperada:
    - Pares clave-valor `source \t score` enviados a través de stdin.
Salida generada:
    - Resultado en formato tabulado con `source` y `promedio`.

Formato de Salida:
    Fuente       Promedio de Puntuaciones
    -------------------------------------
    Manga        8.67
    Original     6.92
    Light Novel  7.85

Ejemplo de uso:
    cat intermediate_output.txt | python3 reducer.py
"""

import sys

def reducer():
    """
    Función principal del reducer. Calcula el promedio de puntuaciones
    para cada fuente (source) usando pares clave-valor.
    """
    # Diccionario para almacenar datos agregados
    data = {}

    # Leer y procesar la entrada estándar (stdin)
    for line in sys.stdin:
        # Dividir la línea en partes usando el tabulador
        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue  # Ignorar líneas mal formateadas

        source, score = parts
        if score == "NULL":
            continue  # Ignorar valores nulos

        try:
            # Intentar convertir el puntaje a un número flotante
            score_val = float(score)
        except ValueError:
            # Ignorar valores no numéricos
            continue

        # Agregar o actualizar los datos para cada fuente
        if source not in data:
            # Inicializar suma y contador
            data[source] = {'sum': score_val, 'count': 1}
        else:
            # Actualizar suma y contador existentes
            data[source]['sum'] += score_val
            data[source]['count'] += 1

    # Emitir encabezados descriptivos
    print("Fuente\tPromedio de Puntuaciones")
    print("-------------------------------")

    # Calcular y emitir resultados finales ordenados por fuente
    for source, values in sorted(data.items()):
        mean_value = values['sum'] / values['count']
        print(f"{source}\t{mean_value:.2f}")

if __name__ == "__main__":
    reducer()
