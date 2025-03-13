import pandas as pd

# Lista de archivos a procesar
files_dataset = [
    "anime-dataset-2023.csv",
    "anime-filtered.csv",
    "final_animedataset.csv",
    "user-filtered.csv",
    "users-details-2023.csv",
    "users-score-2023.csv",
]

# Función para crear versiones cortas de los archivos
def create_short_versions(files, num_lines=200):
    for file_name in files:
        try:
            # Leer las primeras 200 líneas del archivo
            df = pd.read_csv(file_name, nrows=num_lines)
            
            # Crear el nombre del archivo corto
            short_file_name = f"vshort-{file_name}"
            
            # Guardar el archivo corto
            df.to_csv(r"tests/"+short_file_name, index=False)
            print(f"Archivo corto creado: {short_file_name}")
        except FileNotFoundError:
            print(f"Error: El archivo {file_name} no existe.")
        except Exception as e:
            print(f"Error al procesar el archivo {file_name}: {e}")

# Ejecutar la función para crear versiones cortas
create_short_versions(files_dataset)
