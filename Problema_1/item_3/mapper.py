#!/usr/bin/python3
"""
Mapper que procesa los CSV:
  - vshort-users-score-2023.csv (entrada desde STDIN) con columnas: user_id, Username, anime_id, Anime Title, rating.
  - vshort-users-details-2023.csv con columnas: Mal ID, Username, Gender, Birthday, Location, ...
  
Se unen ambas fuentes:
  1. Carga el CSV de detalles usando os.path.expanduser para manejar rutas con "~".
  2. Procesa el CSV de scores acumulando ratings por usuario.
  3. Para cada usuario calcula su promedio de rating y obtiene el país de los detalles.
  
Emite: country <TAB> 1 <TAB> promedio individual
"""

import sys
import csv
import os

def load_user_details(details_path):
    user_details = {}
    # Expandir la ruta para que "~" se convierta en la ruta absoluta
    details_path = os.path.expanduser(details_path)
    try:
        with open(details_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Usar el campo "Mal ID" del CSV de detalles
                user_id = row["Mal ID"].strip()
                # Obtener el país (Location); si está vacío, asignar "Unknown"
                country = row["Location"].strip() if row["Location"].strip() else "Unknown"
                user_details[user_id] = country
    except Exception as e:
        print(f"Error cargando detalles de usuarios desde {details_path}: {e}", file=sys.stderr)
    return user_details

def mapper():
    # Ruta del archivo de detalles
    details_path = "~/hadoop/test/vshort-users-details-2023.csv"
    user_details = load_user_details(details_path)
    
    # Diccionario para acumular rating por usuario: {user_id: [suma_ratings, cantidad]}
    user_ratings = {}
    
    # Procesar el CSV de scores desde STDIN (se espera que tenga cabecera: user_id, Username, anime_id, Anime Title, rating)
    reader = csv.DictReader(sys.stdin)
    for row in reader:
        try:
            user_id = row["user_id"].strip()
            rating = float(row["rating"])
        except KeyError as ke:
            print(f"Error: columna no encontrada en la entrada: {ke}", file=sys.stderr)
            continue
        except ValueError:
            continue
        
        if user_id not in user_ratings:
            user_ratings[user_id] = [0.0, 0]
        user_ratings[user_id][0] += rating
        user_ratings[user_id][1] += 1
    
    # Para cada usuario, calcular el promedio y emitir: país, 1 y promedio individual
    for user_id, (sum_ratings, count_ratings) in user_ratings.items():
        avg_rating = sum_ratings / count_ratings if count_ratings > 0 else 0.0
        country = user_details.get(user_id, "Unknown")
        print(f"{country}\t1\t{avg_rating}")

if __name__ == "__main__":
    mapper()