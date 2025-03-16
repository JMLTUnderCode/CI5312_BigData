#!/usr/bin/python3

import sys
import csv

"""
	Este Mapper esta encargado de recibir la información del archivo 
    "final_animedataset.csv" y realizar la transformación de la información
    tomando dos columnas importantes 
    - En csv "title" = En codigo "anime_title"
    - En csv "gender" = En codigo "user_gender"
    - En csv "popularity" = En codigo "popularity"
    
    Se imprime la tripleta titulo-genero_usuario-popularidad.
    
    Colaboradores en este archivo:
    - Astrid Alvarado
    - Junior Lara
"""

def mapper():
    reader = csv.reader(sys.stdin)
    next(reader)  # Se omite la cabecera
    for row in reader:
        if len(row) > 12:
            anime_title = row[5]  # Columna 'title'
            user_gender = row[4]  # Columna 'gender'
            popularity = row[11]  # Columna 'popularity'
            print(f"{anime_title}\t{user_gender}\t{popularity}")

if __name__ == "__main__":
    mapper()