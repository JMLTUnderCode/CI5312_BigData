#!/usr/bin/python3

"""
	Este Reducer esta encargado de recibir los datos del mapper asociado
    que constituye "localización - usuario" luego realizar un conteo de
	usuarios por localización. De esta forma se obtiene la cantidad de 
    usuarios por pais.
    
    Colaboradores en este archivo:
    - Junior Lara
    - Astrid Alvarado
"""

import sys
from collections import defaultdict

# Reducer para contar usuarios por localización
def reducer():
    user_per_location = defaultdict(list)
    
    for line in sys.stdin:
        parts = line.strip().split("\t")
        
        if len(parts) != 2:
            continue # Si no hay dos partes, se ignora la línea (i.e no hay ubicación)

        location, user_id = parts 
        user_per_location[location].append(user_id)

    for location, users in user_per_location.items():
        print(f"{location}\t{len(users)}\t{users}")
        
if __name__ == "__main__":
    reducer()