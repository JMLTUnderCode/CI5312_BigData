# Problema 4

>[!IMPORTANT]
> Analizar las tendencias de géneros de anime consumidos por los usuarios según su signo zodiacal.
>
> * Item 1: Determinar si existe tendencia de que los estudios publiquen animes en determinado mes del año según la fecha de cumpleaños del público que suele votar por animes que el estudio ha publicado en otras oportunidades.

## **Objetivo:**

Analizar las tendencias de géneros de anime consumidos por los usuarios según su signo zodiacal y determinar si existe una relación entre los meses de publicación de animes por parte de los estudios y las fechas de cumpleaños del público que vota por ellos.

Este análisis busca:

1. Identificar patrones de consumo de géneros de anime asociados a los signos zodiacales.
2. Verificar si los estudios publican animes en determinados meses relacionados con los cumpleaños de los usuarios que suelen calificarlos.

---

## **Índice**

- [Problema 4](#problema-4)
  - [**Objetivo:**](#objetivo)
  - [**Índice**](#índice)
  - [**Ejecución**](#ejecución)
    - [**Archivos Utilizados**](#archivos-utilizados)
    - [**Comandos para Ejecución**](#comandos-para-ejecución)
    - [**Estructura de Carpetas**](#estructura-de-carpetas)
  - [**Resultados**](#resultados)
    - [**Ejemplo de Salida**](#ejemplo-de-salida)
      - [**Top 5 Signos Zodiacales con Más Interacción en el Mes de Publicación**](#top-5-signos-zodiacales-con-más-interacción-en-el-mes-de-publicación)
    - [**Interpretación**](#interpretación)

---

## **Ejecución**

### **Archivos Utilizados**

1. **`mapper.py`**:
   * Calcula el signo zodiacal del usuario basado en el campo `Birthday` del dataset.
   * Genera pares clave-valor en el formato:

     ```bash
     <signo_zodiacal>\t<mes_publicación>\t1
     ```

2. **`reducer.py`**:
   * Agrupa las entradas generadas por el Mapper para calcular la cantidad total de registros por signo zodiacal y mes de publicación.
   * Devuelve resultados en el formato:

     ```bash
     <signo_zodiacal>\t<mes_publicación>\t<total>
     ```

3. **`clean.sh`**:
   * Limpia el directorio de resultados (`output/`) de ejecuciones anteriores:

     ```bash
     #!/bin/bash
     rm -r output/*
     ```

4. **`run.sh`**:
   * Ejecuta el trabajo de Hadoop Streaming con los archivos del Mapper y Reducer:

     ```bash
     #!/bin/bash
     HADOOP_HOME=~/hadoop
     HADOOP_BIN=$HADOOP_HOME/bin/hadoop
     HADOOP_STREAMING_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming.jar

     INPUT_FILE=../tests/vshort-users-details-2023.csv
     OUTPUT_DIR=./output
     MAPPER=./mapper.py
     REDUCER=./reducer.py

     $HADOOP_BIN jar $HADOOP_STREAMING_JAR \
         -input $INPUT_FILE \
         -output $OUTPUT_DIR \
         -mapper "python3 $MAPPER" \
         -reducer "python3 $REDUCER" \
         -file $MAPPER \
         -file $REDUCER
     ```

---

### **Comandos para Ejecución**

1. Limpiar resultados previos:

   ```bash
   ./clean.sh
   ```

2. Ejecutar el trabajo MapReduce:

   ```bash
   ./run.sh
   ```

---

### **Estructura de Carpetas**

Dentro del directorio principal del proyecto, deben existir las siguientes carpetas:

1. **`test/`**:
   * Contiene el archivo `vshort-users-details-2023.csv`.

2. **`output/`**:
   * Almacena los resultados generados por el Reducer.

3. **`Problema_4/`**:
   * Incluye los scripts `mapper.py` y `reducer.py`.

---

## **Resultados**

### **Ejemplo de Salida**

#### **Top 5 Signos Zodiacales con Más Interacción en el Mes de Publicación**

| Signo Zodiacal | Mes Publicación | Total de Votos |
|----------------|-----------------|----------------|
| Aries          | 3               | 15             |
| Pisces         | 3               | 12             |
| Aquarius       | 1               | 10             |
| Capricorn      | 1               | 9              |
| Libra          | 10              | 8              |

### **Interpretación**

Los resultados muestran que ciertos signos zodiacales tienen más interacción con animes publicados en determinados meses, lo que podría reflejar patrones de consumo influenciados por la fecha de nacimiento.
