#!/usr/bin/python3

import sys
import csv

def mapper():
    reader = csv.reader(sys.stdin)
    next(reader)  # Skip header
    for row in reader:
        if len(row) > 12:
            anime_title = row[5]  # Assuming 'anime_id' is the 1st column
            user_gender = row[4]  # Assuming 'gender' is the 5th column
            popularity = row[11]  # Assuming 'popularity' is the 6th column
            print(f"{anime_title}\t{user_gender}\t{popularity}")

if __name__ == "__main__":
    mapper()