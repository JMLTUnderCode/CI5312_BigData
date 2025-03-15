#!/usr/bin/python3

"""
	Este Mapper esta encargado de recibir la información del archivo 
    "final_animedataset.csv" y realizar la transformación de la información
    tomando dos columnas importantes 
    - En csv "gender" = En codigo "user_gender"
    - En csv "genre" = En codigo "genres"
    
    Posteriormente se realiza un split de la columna "genre" para obtener
    los generos asociados de cada anime y se imprime el par genero_anime-genero_usuario.
    
    Colaboradores en este archivo:
    - Astrid Alvarado
"""

import sys
import csv

def mapper():
    reader = csv.reader(sys.stdin)
    next(reader)  # Se omite la cabecera
    for row in reader:
        if len(row) > 12:
            user_gender = row[4]  # Columna 'gender'
            genres = row[12]  # Columna 'genre'

            for genre in genres.split(','):
                genre = genre.strip()   # Se elimina espacios en blanco
                if genre:
                    print(f"{genre}\t{user_gender}")

if __name__ == "__main__":
    mapper()