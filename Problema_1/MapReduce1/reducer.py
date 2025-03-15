#!/usr/bin/python3

import sys

# Reducer para combinar datos de usuarios y ubicaciones
def reducer():
    current_user_id = None
    current_location = None

    for line in sys.stdin:
        parts = line.strip().split("\t")
        
        if len(parts) != 2:
            continue # Si no hay dos partes, se ignora la línea (i.e no hay ubicación)

        user_id, location = parts 
        
        # Si el usuario es el mismo, se ignora
        if current_user_id == user_id:
            continue
        else:
            # Si el usuario cambia, se imprime el usuario y su ubicación
            if current_user_id:
                print(f"{current_user_id}\t{current_location}")
            # Se actualiza el usuario y la ubicación
            current_user_id = user_id
            current_location = location

    # Se imprime el último usuario y su ubicación
    if current_user_id == user_id:
        print(f"{current_user_id}\t{current_location}")

if __name__ == "__main__":
    reducer()