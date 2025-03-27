#!/usr/bin/python3

"""
    Este Mapper esta encargado de recibir la información del archivo
    "users-score-2023.csv" y realizar un filtrado de campos como lo son
    - En csv "user_id" = En codigo "user_id"
    - En csv "rating" = En codigo "rating"
    
    Obteniendo como clave a los usuarios y como valor la calificación que
    le dieron a un anime.
    
    Colaboradores en este archivo:
    - Junior Lara
    - Astrid Alvarado
"""

import sys
import csv

# Mapper
def mapper():
    reader = csv.reader(sys.stdin)
    next(reader)  # Saltar la cabecera
    for row in reader:
        user_id = row[0]
        rating = row[4]
        print(f"{user_id}\t{rating}")

if __name__ == "__main__":
    mapper()