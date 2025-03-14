# Reducer para combinar datos de usuarios y ubicaciones

import sys
from collections import defaultdict

def reducer():
    user_locations = {}
    
    for line in sys.stdin:
        line = line.strip()
        user_id, location = line.split('\t')
        user_locations[user_id] = location
    
    for user_id, location in user_locations.items():
        print(f"{user_id}\t{location}")

if __name__ == "__main__":
    reducer()