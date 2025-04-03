#!/usr/bin/python3

"""
Mapper para identificar patrones de consumo de géneros de anime según los signos zodiacales.

Entrada:
- `vshort-users-details-2023.csv` desde STDIN.
- `vshort-anime-filtered.csv` cargado externamente.

Salida:
- Emitir pares clave-valor: `<signo_zodiacal>\t<género>`.
"""

import sys
import csv
from datetime import datetime
import os

def get_zodiac_sign(month, day):
    """Determina el signo zodiacal basado en el mes y día de nacimiento."""
    zodiac = [
        ("Capricorn", (12, 22), (1, 19)),
        ("Aquarius", (1, 20), (2, 18)),
        ("Pisces", (2, 19), (3, 20)),
        ("Aries", (3, 21), (4, 19)),
        ("Taurus", (4, 20), (5, 20)),
        ("Gemini", (5, 21), (6, 20)),
        ("Cancer", (6, 21), (7, 22)),
        ("Leo", (7, 23), (8, 22)),
        ("Virgo", (8, 23), (9, 22)),
        ("Libra", (9, 23), (10, 22)),
        ("Scorpio", (10, 23), (11, 21)),
        ("Sagittarius", (11, 22), (12, 21)),
    ]
    for sign, start, end in zodiac:
        if (month == start[0] and day >= start[1]) or (month == end[0] and day <= end[1]):
            return sign
    return "Unknown"

def load_anime_genres(file_path):
    """Carga los géneros de animes desde el archivo `vshort-anime-filtered.csv`."""
    anime_genres = {}
    file_path = os.path.expanduser(file_path)
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                anime_id = row["anime_id"].strip()
                genres = row["Genres"].strip()
                anime_genres[anime_id] = genres
    except Exception as e:
        print(f"Error cargando datos de géneros: {e}", file=sys.stderr)
    return anime_genres

def mapper():
    # Ruta al archivo de géneros de anime
    anime_file_path = "~/CI5312_BigData/tests/anime-filtered.csv"
    anime_genres = load_anime_genres(anime_file_path)

    # Leer el archivo de usuarios desde STDIN
    reader = csv.DictReader(sys.stdin)
    for row in reader:
        try:
            # Obtener el signo zodiacal del usuario
            birthday = row["Birthday"]
            if birthday:
                birthday_date = datetime.fromisoformat(birthday)
                zodiac_sign = get_zodiac_sign(birthday_date.month, birthday_date.day)
            else:
                zodiac_sign = "Unknown"
            
            # Relacionar el usuario con géneros de animes
            user_anime_id = row["Mal ID"].strip()
            if user_anime_id in anime_genres:
                genres = anime_genres[user_anime_id]
                # Emitir clave-valor: signo zodiacal y género
                for genre in genres.split(","):
                    print(f"{zodiac_sign}\t{genre.strip()}")
        except Exception as e:
            continue

if __name__ == "__main__":
    mapper()
