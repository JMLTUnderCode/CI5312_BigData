#!/usr/bin/python3
"""
Reducer que recibe, por cada línea, datos en el formato:
    country <TAB> count <TAB> average
Cada línea corresponde a un usuario (obtenido del mapper) con su promedio individual.
El Reducer agrupa por país sumando la cantidad de usuarios y la suma ponderada de ratings,
para luego calcular el promedio final de cada país.
Finalmente, se muestran:
  - Top 10 países con puntuaciones promedio altas
  - Top 10 países con puntuaciones promedio bajas

Solo se consideran países con al menos 1000 usuarios.
"""

import sys

def reducer():
    country_dict = {}
    # Procesar cada línea de entrada
    for line in sys.stdin:
        try:
            country, count_str, avg_str = line.strip().split('\t')
            count = int(count_str)
            avg = float(avg_str)
            # Combina resultados para el mismo país (ponderando por count)
            if country in country_dict:
                prev_count, prev_weight = country_dict[country]
                new_count = prev_count + count
                new_weight = prev_weight + (avg * count)
                country_dict[country] = (new_count, new_weight)
            else:
                country_dict[country] = (count, avg * count)
        except Exception as e:
            print(f"Error al procesar línea: {line.strip()} Error: {str(e)}", file=sys.stderr)
    
    # Calcular el promedio final para cada país solo si tienen al menos 1000 usuarios
    results = []
    for country, (total_count, total_weight) in country_dict.items():
        if total_count >= 2:
            final_avg = total_weight / total_count if total_count > 0 else 0.0
            results.append((country, total_count, final_avg))
    
    # Ordenar para obtener los Top 10 de cada lista
    top_high = sorted(results, key=lambda x: x[2], reverse=True)[:10]
    top_low = sorted(results, key=lambda x: x[2])[:10]
    
    # Imprimir los resultados
    print("Top 10 países con puntuaciones promedio altas:")
    print(f"Country\t# Users\tAverage Score")
    for country, count, avg in top_high:
        print(f"{country}\t{count}\t{avg:.2f}")
    
    print("\nTop 10 países con puntuaciones promedio bajas:")
    print(f"Country\t# Users\tAverage Score")
    for country, count, avg in top_low:
        print(f"{country}\t{count}\t{avg:.2f}")

if __name__ == "__main__":
    reducer()