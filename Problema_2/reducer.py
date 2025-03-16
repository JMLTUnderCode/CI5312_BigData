#!/usr/bin/python3
import sys

def reducer():
    data = {}
    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue
        source, score = parts
        if score == "NULL":
            continue
        try:
            score_val = float(score)
        except ValueError:
            continue
        if source not in data:
            data[source] = {'sum': score_val, 'count': 1}
        else:
            data[source]['sum'] += score_val
            data[source]['count'] += 1

    for source, values in data.items():
        if values['count'] > 0:
            mean_value = values['sum'] / values['count']
            print(f"{source}\t{mean_value}")

if __name__ == "__main__":
    reducer()
