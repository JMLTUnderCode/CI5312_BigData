#!/usr/bin/python3

"""
	Este Reducer esta encargado de recibir los datos del mapper asociado
    que constituye "genero_anime - genero_usuario". Se realiza un conteo
    de la cantidad de hombres y mujeres por genero de anime, para luego
    calcular el porcentaje de hombres y mujeres.
    
    Colaboradores en este archivo:
    - Astrid Alvarado
    - Junior Lara
"""

import sys
from collections import defaultdict

def reducer():
    genre_distribution = defaultdict(lambda: {'male': 0, 'female': 0})

    print("Genre\tMale\tFemale\tMale (%)\tFemale (%)")

    for line in sys.stdin:
        genre, gender = line.strip().split("\t")

        # Se realiza conteo de la cantidad de hombres y mujeres por genero de anime
        if gender == 'Male':
            genre_distribution[genre]['male'] += 1
        elif gender == 'Female':
            genre_distribution[genre]['female'] += 1

    for genre, counts in genre_distribution.items():
        # Se calcula para cada anime el porcentaje de hombres y mujeres
        total = counts['male'] + counts['female']
        male_percentage = (counts['male'] / total) * 100 if total > 0 else 0
        female_percentage = (counts['female'] / total) * 100 if total > 0 else 0

        # se imprime el titulo del anime, la cantidad de hombres y mujeres; y sus porcentajes 
        print(f"{genre}\t{counts['male']}\t{counts['female']}\t{male_percentage:.2f}%\t{female_percentage:.2f}%")

if __name__ == "__main__":
    reducer()