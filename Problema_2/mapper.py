#!/usr/bin/python3

import sys
import csv

def mapper():
    data = []  # renombrado de "list" para evitar conflictos con la función built-in
    reader = csv.reader(sys.stdin)
    try:
        next(reader)  # Saltar la cabecera
    except StopIteration:
        return

    for row in reader:
        # Verificar que la fila tenga al menos 9 columnas
        if len(row) < 9:
            continue
        try:
            # Asignar "NULL" si el campo está vacío
            source = row[7] if row[7] != "" else "NULL"
            score = row[8] if row[8] != "" else "NULL"
            data.append((source, score))
        except Exception as e:
            # En caso de error, ignorar esta fila
            continue
        
    for source, score in data:
        print(f"{source}\t{score}")

if __name__ == "__main__":
    mapper()