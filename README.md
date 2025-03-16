# CI5312_BigData

**Desarrollo de soluciones a un DataSet mediante MapReduce**

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

### Archivo: `"anime-dataset-2023.csv"`

- `anime_id`: ID único para cada anime.
- `Name`: Nombre del anime en su idioma original.
- `English name`: Nombre en inglés.
- `Other name`: Nombre o título en idioma nativo (japonés, chino, etc.).
- `Score`: Calificación o puntuación del anime.
- `Genres`: Géneros del anime, separados por comas.
- `Synopsis`: Resumen breve del argumento del anime.
- `Type`: Tipo de anime (e.g., TV, película, OVA).
- `Episodes`: Número de episodios.
- `Aired`: Fechas de emisión.
- `Source`: Material de origen del anime (e.g., manga, novela ligera, original).
- (Consulta el resto de las columnas en el dataset completo).

### Archivo: `"users-details-2023.csv"`

- `Mal ID`: ID único para cada usuario.
- `Username`: Nombre de usuario.
- `Gender`: Género del usuario.
- `Days Watched`: Días totales viendo anime.
- `Mean Score`: Calificación promedio otorgada por el usuario.
- (Consulta el resto de las columnas en el dataset completo).

### Archivo: `"users-score-2023.csv"`

- `user_id`: ID único para cada usuario.
- `anime_id`: ID único para cada anime.
- `rating`: Calificación otorgada por el usuario al anime.

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
