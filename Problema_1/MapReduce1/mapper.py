# Mapper para combinar datos de usuarios y ubicaciones

import sys
import csv

def mapper():
    reader = csv.reader(sys.stdin)
    for row in reader:
        if len(row) > 1:
            user_id = row[0]
            location = row[4]  # Assuming 'Location' is the 5th column in vshort-users-details-2023.csv
            print(f"{user_id}\t{location}")

if __name__ == "__main__":
    mapper()