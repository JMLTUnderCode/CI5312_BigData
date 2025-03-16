#!/usr/bin/python3

"""
    Este Mapper está encargado de recibir la información del archivo
    "anime-dataset.csv" y realizar la transformación de la información
    tomando varias columnas importantes:
    - "anime" para obtener el nombre del anime
    - "studio" para obtener los estudios de anime
    - "source" para obtener la fuente del anime
    - "genres" para obtener los géneros del anime

    Posteriormente, se realiza un mapeo con clave como el nombre del estudio
    y valor como un diccionario que contiene listas de animes, fuentes y géneros
    asociados a cada estudio.

    Colaboradores en este archivo:
    - Fredthery Castro
"""

import sys
import csv
import json
from collections import defaultdict

def mapper():

    reader = csv.reader(sys.stdin)
    next(reader)  # Se omite la cabecera

    studios = defaultdict(int)

    
    studios = defaultdict(lambda: {"animes": [], "male_points": 0, "female_points": 0, "source": [], "genres": []})

    # Obtenemos una lista de los estudios existentes en el dataset
    for row in reader:
        try:
            # Obtenemos los estudios de anime del dataset
            studios_data = row[14].split(",")
            studios_data = [studio.strip() for studio in studios_data]
            anime = row[1]
            
            for studio in studios_data:
                if anime not in studios[studio]["animes"]:
                    studios[studio]["animes"].append(anime)
                source = row[15]
                genres = row[5].split(",")

                if source not in studios[studio]["source"]:
                    studios[studio]["source"].append(source)

                for genre in genres:
                    genre = genre.strip()
                    if genre not in studios[studio]["genres"]:
                        studios[studio]["genres"].append(genre)
            
        except IndexError:
            # Si ocurre un error al procesar la línea, ignorarla
            continue
       
    # Convertir el diccionario a una cadena de texto usando json.dumps()
    studios_str = json.dumps(studios)
    print(studios_str)
    
if __name__ == "__main__":
    mapper()