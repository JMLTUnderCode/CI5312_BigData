#!/usr/bin/python3

"""
Descripción
Este script es un mapper para Hadoop Streaming que procesa un archivo CSV.

Emite pares clave-valor en formato tabulado (source \t score) y verifica
que los datos relevantes sean válidos antes de procesarlos.

Asume que las columnas relevantes están en las posiciones 7 y 8 del archivo CSV.
"""

import sys
import csv

def mapper():
    # Leer los datos desde la entrada estándar (stdin) como CSV
    reader = csv.reader(sys.stdin)
    try:
        next(reader)  # Saltar la cabecera del archivo CSV
    except StopIteration:
        return  # Si el archivo CSV está vacío, salir de la función

    for row in reader:
        # Verificar que la fila tenga al menos 9 columnas
        if len(row) < 9:
            continue
        try:
            # Extraer 'source' y 'score' y asignar "NULL" si el campo está vacío
            source = row[7].strip() if row[7].strip() else "NULL"
            score = row[8].strip() if row[8].strip() else "NULL"

            # Emitir información procesada en formato tabulado
            print(f"{source}\t{score}")
        except Exception:
            continue  # Ignorar errores en filas mal formateadas

if __name__ == "__main__":
    mapper()
