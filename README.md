# CI5312_BigData

## **Desarrollo de soluciones a un DataSet mediante MapReduce**

Este proyecto utiliza técnicas de procesamiento de datos basadas en MapReduce, implementadas con Hadoop Streaming y Python, para resolver diversos problemas relacionados con un dataset de animes.

- **Link directo al DataSet**: [Anime Dataset 2023](https://www.kaggle.com/datasets/dbdmobile/myanimelist-dataset)

---

## **Indicaciones Previas**

### Autor: Junior Miguel Lara Torres

Para el análisis y preprocesamiento del dataset se utiliza **Python** con la librería **Pandas** para manejar los datos como un DataFrame, facilitando así la manipulación. A continuación, se describen los pasos necesarios para configurar el entorno y trabajar en los problemas:

### 1. Crear un Entorno Virtual en Python

1. Crear el entorno virtual:

   ```bash
   python3 -m venv <mi-entorno>  # Ejemplo: python3 -m venv env
   ```

2. Activar el entorno virtual:

   ```bash
   source <mi-entorno>/bin/activate  # Ejemplo: source env/bin/activate
   ```

3. Para salir del entorno virtual:

   ```bash
   deactivate
   ```

4. Para eliminar el entorno virtual:

   ```bash
   sudo rm -rf <mi-entorno>  # Ejemplo: sudo rm -rf env
   ```

### 2. Instalar Dependencias

El repositorio contiene un archivo `requirements.txt` con las bibliotecas necesarias para el proyecto. Instala las dependencias ejecutando:

```bash
pip install -r requirements.txt
```

### 3. Documentación Adicional

1. Para revisar el nombre e información de los archivos del dataset de Anime 2023:
   - Asegúrate de tener el archivo `kaggle.json` en la carpeta `.kaggle` en tu directorio de usuario (`/home/user`).
   - Luego, ejecuta el comando:

     ```bash
     kaggle datasets files dbdmobile/myanimelist-dataset
     ```

---

## **Contenido del Dataset**

### Archivo: **`anime-dataset-2023.csv`**

Este archivo contiene información detallada sobre los animes disponibles en la base de datos:

- `anime_id`: Unique ID for each anime.
- `Name`: The name of the anime in its original language.
- `English name`: The English name of the anime.
- `Other name`: Native name or title of the anime (can be in Japanese, Chinese, or Korean).
- `Score`: The score or rating given to the anime.
- `Genres`: The genres of the anime, separated by commas.
- `Synopsis`: A brief description or summary of the anime's plot.
- `Type`: The type of the anime (e.g., TV series, movie, OVA, etc.).
- `Episodes`: The number of episodes in the anime.
- `Aired`: The dates when the anime was aired.
- `Premiered`: The season and year when the anime premiered.
- `Status`: The status of the anime (e.g., Finished Airing, Currently Airing, etc.).
- `Producers`: The production companies or producers of the anime.
- `Licensors`: The licensors of the anime (e.g., streaming platforms).
- `Studios`: The animation studios that worked on the anime.
- `Source`: The source material of the anime (e.g., manga, light novel, original).
- `Duration`: The duration of each episode.
- `Rating`: The age rating of the anime.
- `Rank`: The rank of the anime based on popularity or other criteria.
- `Popularity`: The popularity rank of the anime.
- `Favorites`: The number of times the anime was marked as a favorite by users.
- `Scored By`: The number of users who scored the anime.
- `Members`: The number of members who have added the anime to their list on the platform.
- `Image URL`: The URL of the anime's image or poster.

---

### Archivo: **`users-details-2023.csv`**

Este archivo contiene información sobre los usuarios que han interactuado con la plataforma:

- `Mal ID`: Unique ID for each user.
- `Username`: The username of the user.
- `Gender`: The gender of the user.
- `Birthday`: The birthday of the user (in ISO format).
- `Location`: The location or country of the user.
- `Joined`: The date when the user joined the platform (in ISO format).
- `Days Watched`: The total number of days the user has spent watching anime.
- `Mean Score`: The average score given by the user to the anime they have watched.
- `Watching`: The number of anime currently being watched by the user.
- `Completed`: The number of anime completed by the user.
- `On Hold`: The number of anime on hold by the user.
- `Dropped`: The number of anime dropped by the user.
- `Plan to Watch`: The number of anime the user plans to watch in the future.
- `Total Entries`: The total number of anime entries in the user's list.
- `Rewatched`: The number of anime rewatched by the user.
- `Episodes Watched`: The total number of episodes watched by the user.

---

### Archivo: **`users-score-2023.csv`**

Este archivo describe las puntuaciones otorgadas por los usuarios a diferentes animes:

- `user_id`: Unique ID for each user.
- `Username`: The username of the user.
- `anime_id`: Unique ID for each anime.
- `Anime Title`: The title of the anime.
- `rating`: The rating given by the user to the anime.

---

## **Nota sobre Pruebas y Carpeta `tests`**

En la carpeta `tests` del repositorio se incluyen **versiones más ligeras de los archivos CSV originales**. Estas versiones pueden usarse para ejecutar pruebas rápidas en cada uno de los problemas planteados, permitiendo a los estudiantes resolver los ejercicios con el "Hadoop de juguete" en sus computadoras de forma individual. Esto agiliza la experimentación y validación de las soluciones.

---

## **Soluciones Propuestas y Metodología**

Este repositorio está organizado para abordar problemas concretos del análisis de datos utilizando MapReduce. A continuación, una descripción general:

1. **Mapper**:
   - Procesa el dataset y genera pares clave-valor en formato `source \t score`.
   - Utilizado para identificar la relación entre la calidad de los animes y su material de origen.

2. **Reducer**:
   - Calcula el promedio de las puntuaciones para cada fuente (`source`).
   - Genera resultados tabulados claros para análisis.

### Ejecución

#### Limpiar Archivos de Salida

Antes de iniciar un nuevo trabajo de MapReduce, elimina salidas previas con:

```bash
./clean.sh
```

#### Ejecutar MapReduce

Ejecuta el trabajo con:

```bash
./run.sh
```
