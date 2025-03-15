#!/usr/bin/python3

import sys
import csv

# Mapper para combinar datos de ubicaciones y puntuaciones
def mapper():
    reader = csv.reader(sys.stdin)
    for row in reader:
        if len(row) > 1:
            user_id = row[0]
            location = row[1]
            avg_rating = row[2]
            print(f"{location}\t{avg_rating}")

if __name__ == "__main__":
    mapper()