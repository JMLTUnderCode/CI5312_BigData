import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import chardet
import gzip
import os

# Autenticar con la API de Kaggle
api = KaggleApi()
api.authenticate()

# Especifica el dataset y el archivo que deseas leer
dataset = "dbdmobile/myanimelist-dataset"
files_dataset = [
	"anime-dataset-2023.csv",
	"anime-filtered.csv",
	"final_animedataset.csv",
	"user-filtered.csv",
	"users-details-2023.csv",
	"users-score-2023.csv",
]

files_to_Read = [
	"anime-dataset-2023.csv",
]  # Incluir los nombres de los archivos que deseas leer

# Descargar un archivo especifico del dataset 
#api.dataset_download_file(dataset, files_to_read[0], path="./", force=True)

for file in files_dataset:
	if file in files_to_Read:# Detectar la codificación del archivo CSV
		with open(file, 'rb') as f:
			result = chardet.detect(f.read(100000))  # Lee solo los primeros 100,000 bytes para mayor eficiencia

		encoding = result['encoding']
		try:
			df = pd.read_csv(file, nrows=10, encoding=encoding)
			print(f"Archivo leído correctamente con codificación: {encoding}")
			print(df)
		except UnicodeDecodeError:
			print(f"Error al leer con codificación: {encoding}")
		except Exception as e:
			print(f"Error inesperado con codificación {encoding}: {e}")