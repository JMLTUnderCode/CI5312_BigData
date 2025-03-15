#!/usr/bin/python3

"""
	Este Mapper esta encargado de recibir la información del archivo 
    "users-details-2023.csv" y realizar la transformación de la información
    tomando dos columnas importantes 
    - En csv "Mal ID" = En codigo "user_id"
    - En csv "location" = En codigo "location"
    
    Se realiza un filtro y limpieza del campo localizacion ya que el mismo es de 
    escritura arbitraria. Para este mapper se toma en cuenta únicamente el país,
    por lo que no se trabaja con ciudades, estados, entre otras.
    
    Existe la posibilidad, dado la naturaleza del campo, que existen abreviaturas
    de los paises, por tanto se tiene un diccionario de las abreviaturas de los 
    paises y un manejo adecuado para trabajar en casos con simbolos adicionales 
	en los mismos.
    
    Colaboradores en este archivo:
    - Junior Lara
    - Astrid Alvarado
"""

import sys
import csv
sys.path.insert(0, "lib") 
import pycountry
import re

country_abbreviations = {
    "usa": "united states",
    "uk": "united kingdom",
    "uae": "united arab emirates",
    "prc": "china",
    "us": "united states",
    "gb": "united kingdom",
    "ru": "russia",
    "ca": "canada",
    "au": "australia",
    "nz": "new zealand",
    "de": "germany",
    "fr": "france",
    "es": "spain",
    "it": "italy",
    "jp": "japan",
    "cn": "china",
    "kr": "south korea",
    "br": "brazil",
    "mx": "mexico",
    "in": "india",
    "za": "south africa",
    "se": "sweden",
    "no": "norway",
    "fi": "finland",
    "dk": "denmark",
    "ch": "switzerland",
    "nl": "netherlands",
    "be": "belgium",
    "at": "austria",
    "pl": "poland",
    "ar": "argentina",
    "cl": "chile",
    "co": "colombia",
    "ve": "venezuela",
    "pe": "peru",
    "ph": "philippines",
    "id": "indonesia",
    "my": "malaysia",
    "sg": "singapore",
    "th": "thailand",
    "vn": "vietnam",
    "sa": "saudi arabia",
    "eg": "egypt",
    "tr": "turkey",
    "gr": "greece",
    "pt": "portugal",
    "cz": "czech republic",
    "sk": "slovakia",
    "hu": "hungary",
    "ro": "romania",
    "bg": "bulgaria",
    "rs": "serbia",
    "hr": "croatia",
    "si": "slovenia",
    "ua": "ukraine",
    "by": "belarus",
    "lt": "lithuania",
    "lv": "latvia",
    "ee": "estonia",
    "is": "iceland",
    "ie": "ireland",
    "pk": "pakistan",
    "bd": "bangladesh",
    "lk": "sri lanka",
    "np": "nepal",
    "af": "afghanistan",
    "ir": "iran",
    "iq": "iraq",
    "sy": "syria",
    "jo": "jordan",
    "lb": "lebanon",
    "il": "israel",
    "ps": "palestine",
    "kw": "kuwait",
    "qa": "qatar",
    "bh": "bahrain",
    "om": "oman",
    "ye": "yemen",
    "dz": "algeria",
    "ma": "morocco",
    "tn": "tunisia",
    "ly": "libya",
    "sd": "sudan",
    "ng": "nigeria",
    "gh": "ghana",
    "ke": "kenya",
    "tz": "tanzania",
    "ug": "uganda",
    "zm": "zambia",
    "zw": "zimbabwe",
    "bw": "botswana",
    "na": "namibia",
    "ao": "angola",
    "mz": "mozambique",
    "mg": "madagascar",
    "et": "ethiopia",
    "sn": "senegal",
    "ci": "ivory coast",
    "cm": "cameroon",
    "cd": "congo",
    "cg": "republic of the congo",
    "ga": "gabon",
    "gq": "equatorial guinea",
    "cv": "cape verde",
    "st": "sao tome and principe",
    "sc": "seychelles",
    "mu": "mauritius",
    "km": "comoros",
    "dj": "djibouti",
    "er": "eritrea",
    "so": "somalia",
    "rw": "rwanda",
    "bi": "burundi",
    "mw": "malawi",
    "ls": "lesotho",
    "sz": "eswatini",
    "sl": "sierra leone",
    "lr": "liberia",
    "gm": "gambia",
    "gw": "guinea-bissau",
    "gn": "guinea",
    "ml": "mali",
    "bf": "burkina faso",
    "ne": "niger",
    "td": "chad",
    "mr": "mauritania",
    "bj": "benin",
    "tg": "togo",
    "cf": "central african republic",
    "ss": "south sudan",
    "sz": "eswatini",
    "bt": "bhutan",
    "mv": "maldives",
    "kh": "cambodia",
    "la": "laos",
    "mm": "myanmar",
    "bn": "brunei",
    "tl": "timor-leste",
    "pg": "papua new guinea",
    "fj": "fiji",
    "ws": "samoa",
    "to": "tonga",
    "vu": "vanuatu",
    "sb": "solomon islands",
    "ki": "kiribati",
    "tv": "tuvalu",
    "nr": "nauru",
    "pw": "palau",
    "mh": "marshall islands",
    "fm": "micronesia",
    "as": "american samoa",
    "gu": "guam",
    "mp": "northern mariana islands",
    "ck": "cook islands",
    "nu": "niue",
    "wf": "wallis and futuna",
    "pf": "french polynesia",
    "nc": "new caledonia",
    "tk": "tokelau",
    "pn": "pitcairn islands",
    "gs": "south georgia and the south sandwich islands",
    "sh": "saint helena",
    "fk": "falkland islands",
    "ai": "anguilla",
    "bm": "bermuda",
    "vg": "british virgin islands",
    "ky": "cayman islands",
    "ms": "montserrat",
    "tc": "turks and caicos islands",
    "vi": "us virgin islands",
    "pr": "puerto rico",
    "gu": "guam",
    "mp": "northern mariana islands",
    "as": "american samoa",
    "um": "united states minor outlying islands",
    "hk": "hong kong",
    "mo": "macau",
    "tw": "taiwan",
    "fo": "faroe islands",
    "gl": "greenland",
    "ax": "aland islands",
    "je": "jersey",
    "gg": "guernsey",
    "im": "isle of man",
    "yt": "mayotte",
    "re": "reunion",
    "mq": "martinique",
    "gp": "guadeloupe",
    "bl": "saint barthelemy",
    "mf": "saint martin",
    "pm": "saint pierre and miquelon",
    "wf": "wallis and futuna",
    "nc": "new caledonia",
    "pf": "french polynesia",
    "tf": "french southern territories",
    "bv": "bouvet island",
    "hm": "heard island and mcdonald islands",
    "aq": "antarctica",
    "cw": "curacao",
    "sx": "sint maarten",
    "bq": "caribbean netherlands",
    "xk": "kosovo",
}

# Obtener lista de países
countries = [country.name.lower() for country in pycountry.countries]

# Obtener lista de subdivisiones (estados/provincias)
#subdivisions = [subdivision.name.lower() for subdivision in pycountry.subdivisions]

# Mapper para combinar datos de usuarios y ubicaciones
def mapper():
    user_location = []  # Lista para almacenar pares (user_id, location)
    reader = csv.reader(sys.stdin)
    next(reader)  # Saltar la cabecera
    for row in reader:
        try:
            user_id = int(row[0])  # Columna 'Mal ID'
            location = row[4].strip()  # Columna 'Location'
            
            # Si la ubicación está vacía, asignar "desconocido"
            if not location:
                location = "desconocido"
            else:
                location = location.lower()  # Convertir a minúsculas para uniformidad
                
				# Eliminar puntos de las abreviaciones
                location = re.sub(r'\.', '', location)
                
				# Dividir la ubicación en palabras significativas usando expresiones regulares
                location_words = re.findall(r'\b\w+\b', location)
                
                # Verificar si alguna palabra está en el diccionario de abreviaturas
                location_words = [country_abbreviations.get(word, word) for word in location_words]

                # Verificar si alguna palabra está en países
                matched_country = next((word for word in location_words if word in countries), None)
                if matched_country:
                    location = matched_country  # Asignar el país encontrado como la ubicación
                else:
                    location = "inconcluso"  # Ninguna palabra coincide con un país
        
            user_location.append((user_id, location))  # Agregar a la lista como tupla
        except ValueError:
            # Si no se puede convertir user_id a entero, ignorar la fila
            continue
    
	# Retornar la información vía streaming.
    for user_id, location in sorted(user_location):
        print(f"{location}\t{user_id}")

if __name__ == "__main__":
    mapper()