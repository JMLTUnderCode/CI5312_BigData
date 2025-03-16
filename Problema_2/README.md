
# Análisis de la Relación entre Calidad del Anime y su Origen

Este proyecto tiene como objetivo analizar si la calidad de los animes (medida a través de sus puntuaciones) está relacionada con su origen, como ser adaptaciones de mangas, producciones animadas originales, novelas ligeras u otras fuentes.

## Estructura del Proyecto

El proyecto utiliza Hadoop Streaming para procesar datos a través de un trabajo MapReduce. A continuación, se describen los scripts y su propósito:

- **mapper.py**: Extrae las columnas relevantes de un dataset de anime y las emite como pares clave-valor (`source`, `score`).
- **reducer.py**: Calcula el promedio de las puntuaciones para cada fuente (`source`) agregada por el mapper.

## Requisitos Previos

Antes de ejecutar el proyecto, asegúrate de tener instalados y configurados los siguientes requisitos:

- **Hadoop** (versión 3.4.1 o compatible).
- **Python 3.x** con los módulos estándar (no se requieren bibliotecas externas).
- Un dataset en formato CSV con las columnas relevantes.

## Dataset

El dataset utilizado contiene información sobre animes y sus puntuaciones. Las columnas relevantes son:

- **source**: Fuente del anime (por ejemplo, "manga", "novel", "original").
- **score**: Puntuación del anime.

## Ejecución del Proyecto

Para ejecutar este proyecto correctamente, sigue estos pasos:

### 1. Limpiar Salidas Previas

Antes de ejecutar el trabajo de MapReduce, elimina cualquier archivo de salida existente para evitar conflictos. Usa el siguiente script:

```bash
# filepath: /home/jose/CI5312_BigData/Problema_2/clean.sh
rm output/* -r  # Elimina los archivos de salida anteriores
```

### 2. Ejecutar el Trabajo MapReduce

Usa el siguiente script para ejecutar el trabajo de Hadoop Streaming:

```bash
# filepath: /home/jose/CI5312_BigData/Problema_2/run.sh
# Configuración de variables
HADOOP_HOME=~/hadoop                     # Ruta al directorio de Hadoop
REPO_DIR=../                             # Ruta al repositorio del proyecto
HADOOP_BIN=$HADOOP_HOME/bin/hadoop       # Ruta al binario de Hadoop
HADOOP_STREAMING_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar  # JAR de Hadoop Streaming

# Dataset y configuración del problema
DATASET_DIR=$REPO_DIR/tests
INPUT_FILE=$DATASET_DIR/vshort-final_animedataset.csv  # Archivo de entrada
OUTPUT_DIR=./output                                   # Directorio de salida
ACTUAL_PROBLEM=$REPO_DIR/Problema_2                  # Directorio actual del problema
MAPPER_DIR=$ACTUAL_PROBLEM/mapper.py                 # Script del mapper
REDUCER_DIR=$ACTUAL_PROBLEM/reducer.py               # Script del reducer

# Ejecución de Hadoop Streaming
$HADOOP_BIN jar $HADOOP_STREAMING_JAR \
    -input $INPUT_FILE \
    -output $OUTPUT_DIR \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file $MAPPER_DIR \
    -file $REDUCER_DIR
```

Este script configura las rutas y ejecuta el trabajo de MapReduce, emitiendo los resultados en el directorio de salida especificado.

### 3. Verificar los Resultados

Los resultados se almacenan en el directorio `output` y contienen el promedio de las puntuaciones agrupadas por su fuente (`source`). Puedes inspeccionar los resultados con:

```bash
cat output/part-00000
```

## Notas

- **Rendimiento**: Este proyecto es ideal para datasets pequeños o medianos. Para grandes volúmenes de datos, considera ajustes en la configuración de Hadoop.
- **Extensibilidad**: Puedes modificar los scripts para incluir análisis adicionales, como desviación estándar o visualización de resultados.
