# Mapper para combinar datos de usuarios y sus puntuaciones

import sys
import csv

def mapper():
    reader = csv.reader(sys.stdin)
    for row in reader:
        if len(row) > 1:
            user_id = row[0]
            rating = row[4]  # Assuming 'rating' is the 5th column in vshort-users-score-2023.csv
            print(f"{user_id}\t{rating}")

if __name__ == "__main__":
    mapper()