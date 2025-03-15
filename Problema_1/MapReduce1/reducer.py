#!/usr/bin/python3

import sys
from collections import defaultdict

# Reducer para combinar datos de usuarios y ubicaciones
def reducer():
    user_per_location = defaultdict(list)
    
    current_user_id = None
    
    for line in sys.stdin:
        parts = line.strip().split("\t")
        
        if len(parts) != 2:
            continue # Si no hay dos partes, se ignora la línea (i.e no hay ubicación)

        location, user_id = parts 
        
        # Si el usuario es el mismo, se ignora
        if current_user_id == user_id:
            continue
        else:
            user_per_location[location].append(user_id)
            current_user_id = user_id

    for location, users in user_per_location.items():
        print(f"{location}\t{len(users)}")
        
if __name__ == "__main__":
    reducer()