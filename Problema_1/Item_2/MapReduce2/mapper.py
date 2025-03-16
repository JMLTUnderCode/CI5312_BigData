#!/usr/bin/python3

"""
    Este Mapper esta encargado de recibir la información del archivo 
    generado por el ITEM 1 (listado y cantidad de usuarios por pais) el 
    cual se encuentra en la carpeta "output/output_item1" y tiene los 
    siguientes campos
    - Pais
    - Cantidad de usuarios
    - Lista de usuarios
    
    Adicionalmente, se recibe el archivo generado por el Job 1 
    (promediador de ratings por usuario) que se encuenta en la carpeta 
    "output/output_item2_p1" que contiene los siguientes campos
    - user_id
    - average
    
    El objetivo de este Mapper es combinar la información de ambos 
    archivos para obtener el promedio total de los promedios individuales
    de cada usuario que pertenece a determinado pais.
    
    Retorno:
    - country: Pais
    - count: Cantidad de usuarios en el pais.
    - average: Puntacion promedio por pais.
    
    Colaboradores en este archivo:
    - Junior Lara
    - Astrid Alvarado
"""

import sys
import os
import glob
from collections import defaultdict

# Leer múltiples archivos y crear un diccionario de user_id -> average
def load_user_averages(output_dir):
    user_averages = {}
    # Buscar todos los archivos part-* en el directorio de salida
    for file_path in glob.glob(os.path.join(output_dir, "part-*")):
        with open(file_path, "r") as file:
            for line in file:
                try:
                    user_id, average = line.strip().split('\t')
                    user_averages[user_id] = float(average)  # Convertir average a float
                except ValueError:
                    # Si ocurre un error al procesar la línea, ignorarla
                    print(f"Error al procesar la línea del archivo: {line.strip()}", file=sys.stderr)
                    continue
    return user_averages

# Mapper para calcular promedio total de puntuacion por pais.
def mapper():
    # Cargar el diccionario de user_id -> average
    user_averages = load_user_averages("output/p1-item2-part1")
    country_avg = defaultdict(list)
    
    for line in sys.stdin:
        try:
            country, count, users = line.strip().split('\t')
            
            # Convertir el string de users en una lista
            users = users.strip("[]").replace("'", "").split(", ")
            
            # Iterar sobre los usuarios y buscar su promedio en el diccionario
            sum = 0
            total = 0
            for user in users:
                average = user_averages.get(user, -1)  # Obtener el promedio o "0" si no existe
                sum += average if average != -1 else 0
                total += 1 if average != -1 else 0

            avg_total = (sum/total) if total != 0 else 0
            print(f"{country}\t{count}\t{avg_total}")
            
        except ValueError:
            # Si ocurre un error al dividir la línea, ignorar la fila
            print(f"Error al dividir la línea: {line.strip()}", file=sys.stderr)
            continue

if __name__ == "__main__":
    mapper()