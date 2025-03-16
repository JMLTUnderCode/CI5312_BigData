# Variables de configuración
HADOOP_HOME=~/hadoop  # Ruta al directorio de Hadoop
REPO_DIR=../  # Ruta al repositorio

# Variables de Hadoop
HADOOP_BIN=$HADOOP_HOME/bin/hadoop  # Ruta al binario de Hadoop
HADOOP_STREAMING_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar  # Ruta al jar de Hadoop Streaming

# Variables del dataset
DATASET_DIR=$REPO_DIR/tests
INPUT_FILE=$DATASET_DIR/vshort-users-details-2023.csv

# Variables del problema
OUTPUT_DIR=./output  # Directorio de salida para el primer MapReduce
OUTPUT2_DIR=./output2  # Directorio de salida para el segundo MapReduce
ACTUAL_PROBLEM=$REPO_DIR/Problema_4  # Directorio actual del problema
MAPPER1=$ACTUAL_PROBLEM/mapper.py  # Primer mapper
REDUCER1=$ACTUAL_PROBLEM/reducer.py  # Primer reducer
MAPPER2=$ACTUAL_PROBLEM/mapper2.py  # Segundo mapper
REDUCER2=$ACTUAL_PROBLEM/reducer2.py  # Segundo reducer

# Verificar la existencia de los scripts requeridos
if [[ ! -f $MAPPER1 || ! -f $REDUCER1 ]]; then
  echo "Error: No se encontraron $MAPPER1 o $REDUCER1. Verifica la ruta." >&2
  exit 1
fi

if [[ ! -f $MAPPER2 || ! -f $REDUCER2 ]]; then
  echo "Error: No se encontraron $MAPPER2 o $REDUCER2. Verifica la ruta." >&2
  exit 1
fi

# Limpiar resultados previos si los directorios existen
echo "Limpiando directorios de salida previos (si existen)..."
if [ -d $OUTPUT_DIR ]; then
  rm -r $OUTPUT_DIR
  if [[ $? -ne 0 ]]; then
    echo "Error al eliminar el directorio $OUTPUT_DIR. Verifica los permisos." >&2
    exit 1
  fi
fi

if [ -d $OUTPUT2_DIR ]; then
  rm -r $OUTPUT2_DIR
  if [[ $? -ne 0 ]]; then
    echo "Error al eliminar el directorio $OUTPUT2_DIR. Verifica los permisos." >&2
    exit 1
  fi
fi

# Primer trabajo de Hadoop Streaming (Fase 1)
echo "Ejecutando el primer trabajo de MapReduce..."
$HADOOP_BIN jar $HADOOP_STREAMING_JAR \
    -input $INPUT_FILE \
    -output $OUTPUT_DIR \
    -mapper "python3 $MAPPER1" \
    -reducer "python3 $REDUCER1" \
    -file $MAPPER1 \
    -file $REDUCER1

if [[ $? -ne 0 ]]; then
  echo "Error durante la ejecución del primer trabajo de MapReduce." >&2
  exit 1
fi

echo "Primer trabajo de MapReduce completado. Resultados en $OUTPUT_DIR"

# Preparar la entrada para el segundo trabajo
PART_FILE=$OUTPUT_DIR/part-00000  # Archivo de salida del primer trabajo

# Verificar la existencia del archivo `part-00000`
if [[ ! -f $PART_FILE ]]; then
  echo "Error: No se encontró el archivo $PART_FILE generado por el primer MapReduce." >&2
  exit 1
fi

# Segundo trabajo de Hadoop Streaming (Fase 2)
echo "Ejecutando el segundo trabajo de MapReduce..."
$HADOOP_BIN jar $HADOOP_STREAMING_JAR \
    -input $PART_FILE \
    -output $OUTPUT2_DIR \
    -mapper "python3 $MAPPER2" \
    -reducer "python3 $REDUCER2" \
    -file $MAPPER2 \
    -file $REDUCER2

if [[ $? -ne 0 ]]; then
  echo "Error durante la ejecución del segundo trabajo de MapReduce." >&2
  exit 1
fi

echo "Segundo trabajo de MapReduce completado. Resultados en $OUTPUT2_DIR"
