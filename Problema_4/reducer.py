#!/usr/bin/python3

"""
Reducer para analizar las tendencias de géneros de anime consumidos según
el signo zodiacal y la publicación por meses.

Entrada:
- Formato: `<signo_zodiacal>\t<mes_publicación>\t1`.

Salida:
- Total de registros agrupados por `<signo_zodiacal>` y `<mes_publicación>`.
"""

import sys
from collections import defaultdict

def reducer():
    # Diccionario para agrupar los datos
    data = defaultdict(lambda: defaultdict(int))

    # Procesar cada línea de entrada
    for line in sys.stdin:
        try:
            zodiac_sign, month, count = line.strip().split("\t")
            count = int(count)
            # Agrupar por signo y mes
            data[zodiac_sign][month] += count
        except Exception as e:
            # Si hay algún problema con los datos, ignorar la línea
            continue

    # Emitir resultados agrupados y ordenados
    for zodiac_sign, months in data.items():
        for month, total_count in sorted(months.items(), key=lambda x: int(x[0])):
            print(f"{zodiac_sign}\t{month}\t{total_count}")

if __name__ == "__main__":
    reducer()
