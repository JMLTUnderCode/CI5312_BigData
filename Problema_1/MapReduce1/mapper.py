#!/usr/bin/python3

import sys
import csv
sys.path.insert(0, "lib") 
import pycountry
import re

# Obtener lista de países
countries = [country.name.lower() for country in pycountry.countries]

# Obtener lista de subdivisiones (estados/provincias)
subdivisions = [subdivision.name.lower() for subdivision in pycountry.subdivisions]

# Mapper para combinar datos de usuarios y ubicaciones
def mapper():
    user_location = []  # Lista para almacenar pares (user_id, location)
    reader = csv.reader(sys.stdin)
    next(reader)  # Saltar la cabecera
    for row in reader:
        try:
            user_id = int(row[0])  # Columna 'Mal ID'
            location = row[4].strip()  # Columna 'Location'
            
            # Si la ubicación está vacía, asignar "desconocido"
            if not location:
                location = "desconocido"
            else:
                location = location.lower()  # Convertir a minúsculas para uniformidad
                
                # Dividir la ubicación en palabras significativas usando expresiones regulares
                location_words = re.findall(r'\b\w+\b', location)
                
                # Verificar si alguna palabra está en países
                matched_country = next((word for word in location_words if word in countries), None)
                if matched_country:
                    location = matched_country  # Asignar el país encontrado como la ubicación
                else:
                    location = "inconcluso"  # Ninguna palabra coincide con un país
        
            user_location.append((user_id, location))  # Agregar a la lista como tupla
        except ValueError:
            # Si no se puede convertir user_id a entero, ignorar la fila
            continue
    
    # Ordenar la lista por user_id (que ahora es un entero) y hacer print
    for user_id, location in sorted(user_location):
        print(f"{location}\t{user_id}")

if __name__ == "__main__":
    mapper()