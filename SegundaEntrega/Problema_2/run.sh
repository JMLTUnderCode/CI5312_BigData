# Variables de configuración
HADOOP_HOME=~/hadoop  # Ruta al directorio de Hadoop
REPO_DIR=../  # Ruta al repositorio

# Variables de Hadoop
HADOOP_BIN=$HADOOP_HOME/bin/hadoop  # Ruta al binario de Hadoop
HADOOP_STREAMING_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar  # Ruta al jar de Hadoop Streaming


# Variables del dataset
DATASET_DIR=$REPO_DIR/tests
INPUT_FILE=$DATASET_DIR/vshort-final_animedataset.csv

# Variables del problema
OUTPUT_DIR=./output  # Directorio de salida (se generará en HDFS)
ACTUAL_PROBLEM=$REPO_DIR/Problema_2  # Directorio actual del problema
MAPPER_DIR=$ACTUAL_PROBLEM/mapper.py  # Script del mapper
REDUCER_DIR=$ACTUAL_PROBLEM/reducer.py  # Script del reducer


# Ejecución del trabajo de Hadoop Streaming
$HADOOP_BIN jar $HADOOP_STREAMING_JAR \
    -input $INPUT_FILE \
    -output $OUTPUT_DIR \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file $MAPPER_DIR \
    -file $REDUCER_DIR \
  
echo "Se ejecutó el trabajo de Hadoop Streaming"