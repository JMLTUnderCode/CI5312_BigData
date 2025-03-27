#!/usr/bin/python3

"""
	Este Mapper esta encargado de recibir la información del archivo 
    "final_animedataset.csv" y realizar la transformación de la información
    tomando dos columnas importantes 
    - En csv "gender" = En codigo "user_gender"
    - En csv "source" = En codigo "source"
    
    Posteriormente se realizar un mapeo con clave source y
    valor genero para asi en el reducer tener informacion
    de generos por source.
    
    Colaboradores en este archivo:
    - Junior Lara
    - Astrid Alvarado
"""

import sys
import csv

def mapper():
    reader = csv.reader(sys.stdin)
    next(reader)  # Se omite la cabecera
    for row in reader:
        if len(row) > 7:
            user_gender = row[4]  # Columna 'gender'
            source = row[7]  # Columna 'genre'
            print(f"{source}\t{user_gender}")

if __name__ == "__main__":
    mapper()