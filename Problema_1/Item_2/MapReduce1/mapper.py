#!/usr/bin/python3

"""
	Este Mapper 
    
    Colaboradores en este archivo:
    - Junior Lara
    - Astrid Alvarado
"""

import sys
import csv

# Mapper para combinar datos de usuarios y sus puntuaciones
def mapper():

    for line in sys.stdin:
        try:
            country, count, users = line.split('\t')
            
        except ValueError:
            # Si no se puede convertir user_id a entero, ignorar la fila
            continue
        
if __name__ == "__main__":
    mapper()