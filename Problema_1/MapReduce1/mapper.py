#!/usr/bin/python3

import sys

# Mapper para combinar datos de usuarios y ubicaciones
def mapper():
	for line in sys.stdin:
		splits = line.strip().split(",")
		user_id = splits[0]
		location = splits[4]  # Assuming 'Location' is the 5th column in vshort-users-details-2023.csv
		print(f"{user_id}\t{location}") # Se separan los datos por tabulador para mejor manejo en el reducer

if __name__ == "__main__":
	mapper()