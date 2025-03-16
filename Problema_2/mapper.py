#!/usr/bin/python3

"""
Descripción
Este script es un mapper para Hadoop Streaming que procesa un archivo CSV.

Emite pares clave-valor en formato tabulado (source \t score).

Asume que las columnas relevantes están en las posiciones 7 y 8 del archivo CSV.
"""

import sys
import csv

# Función principal para el mapper
def mapper():
    # Lista para almacenar pares (source, score)
    data = []  # Se usa "data" en lugar de "list" para evitar conflictos con la función built-in
    
    # Leer los datos desde la entrada estándar (stdin) como CSV
    reader = csv.reader(sys.stdin)
    try:
        next(reader)  # Saltar la cabecera del archivo CSV
    except StopIteration:
        # Si el archivo CSV está vacío, salir de la función
        return

    # Iterar por cada fila del archivo CSV
    for row in reader:
        # Verificar que la fila tenga al menos 9 columnas para evitar errores de índice
        if len(row) < 9:
            continue
        try:
            # Asignar "NULL" si la columna está vacía, de lo contrario usar el valor original
            source = row[7] if row[7] != "" else "NULL"
            score = row[8] if row[8] != "" else "NULL"
            # Agregar el par (source, score) a la lista
            data.append((source, score))
        except Exception:
            # Ignorar errores en filas mal formateadas
            continue
    
    # Emitir cada par (source, score) en la salida estándar
    for source, score in data:
        print(f"{source}\t{score}")

# Punto de entrada del script
if __name__ == "__main__":
    mapper()
