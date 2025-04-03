#!/usr/bin/python3

"""
Segundo Mapper para relacionar los géneros populares con los meses de publicación.

Entrada:
- Archivo `part-00000` (salida del primer MapReduce).
- Archivo `vshort-anime-filtered.csv` (información de meses de publicación).

Salida:
- Emitir pares clave-valor: `<signo_zodiacal>\t<mes_publicación>\t1`.
"""

import sys
import csv
from datetime import datetime
import os

def load_anime_publication_data(file_path):
    """Carga los meses de publicación de los animes desde el archivo `vshort-anime-filtered.csv`."""
    anime_months = {}
    file_path = os.path.expanduser(file_path)
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                anime_id = row["anime_id"].strip()
                try:
                    # Obtener el mes de publicación desde el campo "Aired"
                    aired_date = row["Aired"].split("to")[0].strip()  # Primera fecha en caso de rango
                    aired_month = datetime.strptime(aired_date, "%b %d, %Y").month
                    anime_months[anime_id] = aired_month
                except:
                    continue
    except Exception as e:
        print(f"Error cargando meses de publicación: {e}", file=sys.stderr)
    return anime_months

def mapper():
    # Ruta al archivo de animes
    anime_file_path = "~/CI5312_BigData/tests/anime-filtered.csv"
    anime_months = load_anime_publication_data(anime_file_path)

    # Leer la salida del primer MapReduce desde STDIN
    for line in sys.stdin:
        try:
            zodiac_sign, genre, count = line.strip().split("\t")
            count = int(count)
            
            # Relacionar el género con los meses de publicación (usando datos cruzados)
            for anime_id, month in anime_months.items():
                # Emitir clave-valor: signo zodiacal y mes de publicación
                print(f"{zodiac_sign}\t{month}\t{count}")
        except Exception as e:
            continue

if __name__ == "__main__":
    mapper()
