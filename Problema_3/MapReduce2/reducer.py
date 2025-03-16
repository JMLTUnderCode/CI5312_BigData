#!/usr/bin/python3

import sys
from collections import defaultdict

"""
	Este Reducer esta encargado de recibir los datos del mapper asociado
    que constituye "titulo - genero_usuario - popularidad". Se realiza un conteo
    de la cantidad de hombres y mujeres por titulo de anime, para luego
    calcular el porcentaje de hombres y mujeres. Adicionalmente se imprime
    la popularidad del anime.
    
    Colaboradores en este archivo:
    - Astrid Alvarado
    - Junior Lara
"""

def reducer():
    anime_data = defaultdict(lambda: {'male': 0, 'female': 0, 'popularity': 0})

    print("Anime\tMale\tFemale\tMale (%)\tFemale (%)\tPopularity")

    for line in sys.stdin:
        anime_title, gender, popularity = line.strip().split("\t")
        popularity = int(popularity)
        if gender == 'Male':
            anime_data[anime_title]['male'] += 1
        elif gender == 'Female':
            anime_data[anime_title]['female'] += 1

        if anime_data[anime_title]['popularity'] == 0:
            anime_data[anime_title]['popularity'] = popularity

    for anime_title, data in anime_data.items():
        total = data['male'] + data['female']
        male_percentage = (data['male'] / total) * 100 if total > 0 else 0
        female_percentage = (data['female'] / total) * 100 if total > 0 else 0
        print(f"{anime_title}\t{data['male']}\t{data['female']}\t{male_percentage:.2f}%\t{female_percentage:.2f}%\t{data['popularity']}")

if __name__ == "__main__":
    reducer()