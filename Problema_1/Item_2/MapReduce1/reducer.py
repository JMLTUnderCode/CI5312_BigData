#!/usr/bin/python3

"""
	Este Reducer 
    
    Colaboradores en este archivo:
    - Junior Lara
    - Astrid Alvarado
"""

import sys
from collections import defaultdict

def reducer():
    user_scores = defaultdict(list)

    for line in sys.stdin:
        user_id, score = line.strip().split("\t")
        try:
            score = float(score)
            user_scores[user_id].append(score)
        except ValueError:
            continue

    for user_id, scores in user_scores.items():
        if scores:
            avg_score = sum(scores) / len(scores)
            print(f"{user_id}\t{avg_score}")

if __name__ == "__main__":
    reducer()