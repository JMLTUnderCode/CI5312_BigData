#!/usr/bin/python3

"""
Segundo Reducer para relacionar signos zodiacales con meses de publicación.

Entrada:
- Formato: `<signo_zodiacal>\t<mes_publicación>\t<conteo>`.

Salida:
- Incluye una leyenda explicativa al principio de los resultados.
- Relación agrupada: `<signo_zodiacal>\t<mes_publicación>\t<conteo>`.

Leyenda:
El formato de salida es el siguiente:
  - Signo Zodiacal: El signo zodiacal del usuario que calificó el anime.
  - Mes Publicación: El mes en que el anime fue publicado (1 = Enero, ..., 12 = Diciembre).
  - Conteo: La cantidad de interacciones entre usuarios de ese signo y animes publicados en ese mes.
"""

import sys
from collections import defaultdict

def reducer():
    data = defaultdict(lambda: defaultdict(int))

    # Leer línea por línea desde STDIN
    for line in sys.stdin:
        try:
            zodiac_sign, month, count = line.strip().split("\t")
            month = int(month)
            count = int(count)
            # Agrupar datos
            data[zodiac_sign][month] += count
        except Exception as e:
            continue

    # Emitir leyenda explicativa antes de los resultados
    print("Relación entre signos zodiacales y meses de publicación")
    print("-----------------------------------------------------")
    print("Formato:")
    print("Signo Zodiacal\tMes Publicación\tConteo")
    print("(Mes Publicación: 1 = Enero, ..., 12 = Diciembre)")
    print("-----------------------------------------------------")

    # Emitir resultados agrupados
    for zodiac_sign, months in data.items():
        for month, total_count in sorted(months.items()):
            print(f"{zodiac_sign}\t{month}\t{total_count}")

if __name__ == "__main__":
    reducer()
