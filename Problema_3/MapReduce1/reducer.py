#!/usr/bin/python3

import sys
from collections import defaultdict

def reducer():
    genre_distribution = defaultdict(lambda: {'male': 0, 'female': 0})

    print("Genre\tMale\tFemale\tMale (%)\tFemale (%)")

    for line in sys.stdin:
        genre, gender = line.strip().split("\t")
        if gender == 'Male':
            genre_distribution[genre]['male'] += 1
        elif gender == 'Female':
            genre_distribution[genre]['female'] += 1

    for genre, counts in genre_distribution.items():
        total = counts['male'] + counts['female']
        male_percentage = (counts['male'] / total) * 100 if total > 0 else 0
        female_percentage = (counts['female'] / total) * 100 if total > 0 else 0
        print(f"{genre}\t{counts['male']}\t{counts['female']}\t{male_percentage:.2f}%\t{female_percentage:.2f}%")

if __name__ == "__main__":
    reducer()