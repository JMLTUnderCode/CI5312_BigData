#!/usr/bin/python3

"""
    Este Reducer esta encargado de recibir los datos del mapper asociado
    que constituye "usuario - rating" luego realizar un promedio de
	ratings por usuario.
    
    Colaboradores en este archivo:
    - Junior Lara
    - Astrid Alvarado
"""

import sys
from collections import defaultdict

# Reducer
def reducer():
    users_avg = defaultdict(list)
    for line in sys.stdin:
        user_id, rating = line.strip().split("\t")
        users_avg[user_id].append(float(rating))

    for user_id, ratings in users_avg.items():
        avg = sum(ratings) / len(ratings)
        print(f"{user_id}\t{avg}")
        
if __name__ == "__main__":
    reducer()