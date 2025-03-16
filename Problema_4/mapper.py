#!/usr/bin/python3

"""
Mapper para analizar las tendencias de géneros de anime consumidos según
el signo zodiacal de los usuarios y los meses en los que los estudios publican animes.

Entrada:
- CSV `vshort-users-details-2023.csv` desde STDIN.

Salida:
- Emite: `<signo_zodiacal>\t<mes_publicación>\t1`.
"""

import sys
import csv
from datetime import datetime

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

def mapper():
    reader = csv.DictReader(sys.stdin)
    for row in reader:
        try:
            # Parsear la fecha de cumpleaños
            birthday = row["Birthday"]
            if birthday:
                birthday = datetime.fromisoformat(birthday)
                zodiac_sign = get_zodiac_sign(birthday.month, birthday.day)
                # Emitir pares clave-valor
                # Nota: Aquí, "mes_publicación" podría integrarse como entrada externa de estudios.
                print(f"{zodiac_sign}\t{birthday.month}\t1")
        except Exception as e:
            # Si hay algún problema con los datos, ignorar la fila
            continue

if __name__ == "__main__":
    mapper()
