#!/usr/bin/python3

import sys

def mapper():
    next(sys.stdin)  # Skip header
    for line in sys.stdin:
        fields = line.strip().split(',')
        if len(fields) > 12:
            user_gender = fields[4]  # Assuming 'gender' is the 5th column
            genres = fields[12].strip('"')  # Assuming 'genre' is the 13th column
            for genre in genres.split(','):
                print(f"{genre.strip()}\t{user_gender}")

if __name__ == "__main__":
    mapper()