#!/usr/bin/python3

"""
Mapper para Hadoop Streaming

Este script es un componente del modelo MapReduce. Procesa un archivo CSV con información
sobre animes y sus puntuaciones, y emite pares clave-valor en formato tabulado: `source \t score`.

- `source`: Origen del anime (por ejemplo, "manga", "original", "light novel").
- `score`: Puntuación del anime.

Requisitos:
    - Archivo CSV con al menos 9 columnas.
    - Las columnas relevantes son:
        - Columna 7: Origen (source).
        - Columna 8: Puntuación (score).

El script maneja:
    - Valores nulos: Los campos vacíos se marcan como "NULL".
    - Filas mal formateadas: Estas se ignoran sin interrumpir el procesamiento.

Entrada esperada:
    - Archivo CSV enviado a través de la entrada estándar (stdin).
Salida generada:
    - Pares clave-valor `source \t score` en la salida estándar.

Ejemplo de uso:
    cat dataset.csv | python3 mapper.py
"""

import sys
import csv

def mapper():
    """
    Función principal del mapper. Lee datos CSV desde stdin y emite pares clave-valor.
    """
    # Leer los datos desde la entrada estándar (stdin) como CSV
    reader = csv.reader(sys.stdin)
    try:
        # Saltar la cabecera del archivo CSV
        next(reader)
    except StopIteration:
        # Si el archivo CSV está vacío, salir de la función
        return

    for row in reader:
        # Verificar que la fila tenga al menos 9 columnas
        if len(row) < 9:
            continue
        try:
            # Extraer 'source' y 'score' y limpiar espacios en blanco
            source = row[7].strip() if row[7].strip() else "NULL"
            score = row[8].strip() if row[8].strip() else "NULL"

            # Emitir el par clave-valor
            print(f"{source}\t{score}")
        except Exception:
            # Ignorar errores en filas mal formateadas
            continue

if __name__ == "__main__":
    mapper()
