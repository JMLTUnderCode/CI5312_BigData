#!/usr/bin/python3

import sys

# Mapper para combinar datos de usuarios y sus puntuaciones
def mapper():
    for line in sys.stdin:
        splits = line.strip().split(",")
        user_id = splits[0]
        score = splits[4]  # Assuming 'Score' is the 3rd column in vshort-users-score-2023.csv
        print(f"{user_id}\t{score}")

if __name__ == "__main__":
    mapper()