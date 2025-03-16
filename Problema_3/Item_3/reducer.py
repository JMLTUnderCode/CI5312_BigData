#!/usr/bin/python3

"""
	Este Reducer esta encargado de recibir los datos del mapper asociado
    que constituye "source - genero". Se realiza un conteo de cada source
    de femeninos y masculinos. 
    
    Se retorna finalmente el % de hombres y mujeres en los distintos tipos 
    de source en la industria.
    
    Colaboradores en este archivo:
    - Junior Lara
	- Astrid Alvarado
"""

import sys
from collections import defaultdict

def reducer():
    source_distribution = defaultdict(lambda: {'male': 0, 'female': 0})

    print("Source\tMale (%)\tFemale (%)")

    for line in sys.stdin:
        source, gender = line.strip().split("\t")

        # Se realiza conteo de la cantidad de hombres y mujeres por genero de anime
        if gender == 'Male':
            source_distribution[source]['male'] += 1
        elif gender == 'Female':
            source_distribution[source]['female'] += 1

    for source, counts in source_distribution.items():
        # Se calcula para cada source el porcentaje de hombres y mujeres
        total = counts['male'] + counts['female']
        male_percentage = (counts['male'] / total) * 100 if total > 0 else 0
        female_percentage = (counts['female'] / total) * 100 if total > 0 else 0

        # se imprime el source y sus porcentajes de hombres y mujeres.
        print(f"{source}\t{male_percentage:.2f}%\t{female_percentage:.2f}%")

if __name__ == "__main__":
    reducer()