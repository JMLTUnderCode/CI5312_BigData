#!/usr/bin/python3

import sys
from collections import defaultdict

# Reducer para combinar datos de usuarios y sus puntuaciones
def reducer():
    user_ratings = defaultdict(list)
    
    for line in sys.stdin:
        line = line.strip()
        user_id, rating = line.split('\t')
        try:
            rating = float(rating)
            user_ratings[user_id].append(rating)
        except ValueError:
            continue
    
    for user_id, ratings in user_ratings.items():
        if ratings:
            avg_rating = sum(ratings) / len(ratings)
            print(f"{user_id}\t{avg_rating}")

if __name__ == "__main__":
    reducer()