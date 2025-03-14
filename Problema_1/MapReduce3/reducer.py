#!/usr/bin/python3

import sys
from collections import defaultdict

# Reducer para analizar la distribución geográfica y puntuaciones
def reducer():
    location_ratings = defaultdict(list)
    
    for line in sys.stdin:
        line = line.strip()
        location, avg_rating = line.split('\t')
        try:
            avg_rating = float(avg_rating)
            location_ratings[location].append(avg_rating)
        except ValueError:
            continue
    
    for location, ratings in location_ratings.items():
        if ratings:
            avg_rating = sum(ratings) / len(ratings)
            print(f"{location}\t{len(ratings)}\t{avg_rating}")

if __name__ == "__main__":
    reducer()