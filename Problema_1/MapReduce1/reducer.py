#!/usr/bin/python3

import sys

# Reducer para combinar datos de usuarios y ubicaciones
def reducer():
    for line in sys.stdin:
        line = line.strip()
        splits = line.split(' ')
        print(splits)
        #user_id = splits[0]
        #location = splits[1]
        #print(f"{user_id}\t{location}")
        
if __name__ == "__main__":
    reducer()