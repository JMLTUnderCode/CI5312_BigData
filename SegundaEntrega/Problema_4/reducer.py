#!/usr/bin/python3

"""
Reducer para agrupar los géneros de anime según el signo zodiacal.

Entrada:
- Formato: `<signo_zodiacal>\t<género>`.

Salida:
- Total de géneros por signo zodiacal: `<signo_zodiacal>\t<género>\t<conteo>`.
"""

import sys
from collections import defaultdict

def reducer():
    data = defaultdict(lambda: defaultdict(int))

    # Leer línea por línea desde STDIN
    for line in sys.stdin:
        try:
            zodiac_sign, genre = line.strip().split("\t")
            data[zodiac_sign][genre] += 1
        except Exception as e:
            continue

    # Emitir resultados
    for zodiac_sign, genres in data.items():
        for genre, count in genres.items():
            print(f"{zodiac_sign}\t{genre}\t{count}")

if __name__ == "__main__":
    reducer()
