#!/usr/bin/python3

"""
	Este Reducer esta encargado de recibir los datos del mapper asociado
    a los datos de los estudios y su información. Luego, utiliza funciones
    para cargar los datos de los items 1, 2 y 3 y finalmente se realiza un
    conteo de los puntos obtenidos por cada estudio en base a los datos
    obtenidos en los items anteriores.
    
    Colaboradores en este archivo:
    - Fredthery Castro
"""

import sys
import csv
import os
import glob
import json
from collections import defaultdict

def load_data1(studios, output_dir):
    for file_path in glob.glob(os.path.join(output_dir, "part-*")):
        with open(file_path, "r") as file:
            for line in file:
                anime_name, male, female, male_perc, female_perc, popularity = line.strip().split('\t')
                try:
                    for studio in studios:
                        if anime_name in studios[studio]["animes"]:
                            if male_perc > female_perc:
                                studios[studio]["male_points"] += 1
                            elif male_perc == female_perc:
                                studios[studio]["male_points"] += 1
                                studios[studio]["female_points"] += 1
                            else:
                                studios[studio]["female_points"] += 1
                except ValueError:
                    # Si ocurre un error al procesar la línea, ignorarla
                    print(f"Error al procesar la línea del archivo: {line.strip()}", file=sys.stderr)
                    continue
    return studios


def load_data2(studios, output_dir):
    for file_path in glob.glob(os.path.join(output_dir, "part-*")):
        with open(file_path, "r") as file:
            for line in file:
                
                genre, male, female, male_perc, female_perc = line.strip().split('\t')
                try:
                    for studio in studios:
                        if genre in studios[studio]["genres"]:
                            if male_perc > female_perc:
                                studios[studio]["male_points"] += 1
                            elif male_perc == female_perc:
                                studios[studio]["male_points"] += 1
                                studios[studio]["female_points"] += 1
                            else:
                                studios[studio]["female_points"] += 1
                except ValueError:
                    # Si ocurre un error al procesar la línea, ignorarla
                    print(f"Error al procesar la línea del archivo: {line.strip()}", file=sys.stderr)
                    continue
    return studios


def load_data3(studios, output_dir):
    for file_path in glob.glob(os.path.join(output_dir, "part-*")):
        with open(file_path, "r") as file:
            for line in file:
                
                source, male_perc, female_perc = line.strip().split('\t')
                try:
                    for studio in studios:
                        if source in studios[studio]["source"]:
                            if male_perc > female_perc:
                                studios[studio]["male_points"] += 1
                            elif male_perc == female_perc:
                                studios[studio]["male_points"] += 1
                                studios[studio]["female_points"] += 1
                            else:
                                studios[studio]["female_points"] += 1
                except ValueError:
                    # Si ocurre un error al procesar la línea, ignorarla
                    print(f"Error al procesar la línea del archivo: {line.strip()}", file=sys.stderr)
                    continue
    return studios

def reducer():

    print("Studio\tMale Points\t\tStudio\tFemale Points")

    for line in sys.stdin:
        # Convertir la cadena de texto de vuelta a un diccionario
        studios = json.loads(line.strip())
    
    # Procesamos con los datos del item 1
    studios1 = load_data1(studios, "../Item_1/output")
    
    # Procesamos con los datos del item 2
    studios2 = load_data2(studios1, "../Item_2/output")
    
    # Procesamos con los datos del item 3
    final_studios = load_data3(studios2, "../Item_3/output")

    # Se obtienen los 10 mejores estudios para hombres y mujeres
    top_male_studios = sorted(final_studios.items(), key=lambda x: x[1]["male_points"], reverse=True)[:10]
    top_female_studios = sorted(final_studios.items(), key=lambda x: x[1]["female_points"], reverse=True)[:10]

    # Se imprimen los resultados
    for i in range(10):
        male_studio = top_male_studios[i] if i < len(top_male_studios) else ("", {"male_points": ""})
        female_studio = top_female_studios[i] if i < len(top_female_studios) else ("", {"female_points": ""})
        print(f"{male_studio[0]}\t{male_studio[1]['male_points']}\t\t{female_studio[0]}\t{female_studio[1]['female_points']}")
    
if __name__ == "__main__":
    reducer()