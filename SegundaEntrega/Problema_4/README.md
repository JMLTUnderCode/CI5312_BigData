# **Problema 4**

## **Analizar las tendencias de géneros de anime consumidos por los usuarios según su signo zodiacal.**
>
> **Items principales:**
>
> - Identificar patrones de consumo de géneros de anime asociados a los signos zodiacales.
> - Determinar si existe tendencia de que los estudios publiquen animes en determinados meses del año según la fecha de cumpleaños del público que suele votar por animes publicados anteriormente.

---

## **Objetivo**

Analizar las tendencias de géneros de anime consumidos por los usuarios según su signo zodiacal y determinar si existe una relación entre los meses de publicación de animes por parte de los estudios y las fechas de cumpleaños del público que vota por ellos.

Este análisis busca:

1. Identificar patrones de consumo de géneros de anime asociados a los signos zodiacales.
2. Verificar si los estudios publican animes en determinados meses relacionados con los cumpleaños de los usuarios que suelen calificarlos.

---

## **Índice**

1. [Objetivo](#objetivo)
2. [Archivos Utilizados](#archivos-utilizados)
3. [Ejecución](#ejecución)
4. [Comandos para Ejecución](#comandos-para-ejecución)
5. [Estructura de Carpetas](#estructura-de-carpetas)
6. [Resultados](#resultados)
7. [Interpretación](#interpretación)

---

## **Archivos Utilizados**

1. **`mapper.py`**:
   - Determina el signo zodiacal de cada usuario basado en el campo `Birthday` del archivo `vshort-users-details-2023.csv`.
   - Genera pares clave-valor en el formato:

     ```bash
     <signo_zodiacal>\t<género>
     ```

2. **`reducer.py`**:
   - Agrupa los datos del Mapper 1 para calcular cuántos usuarios consumieron un género específico según su signo zodiacal.
   - Genera resultados en el formato:

     ```bash
     <signo_zodiacal>\t<género>\t<conteo>
     ```

3. **`mapper2.py`**:
   - Lee la salida del primer MapReduce (`part-00000`) e identifica la relación entre los géneros y los meses de publicación.
   - Cruza los datos del archivo `vshort-anime-filtered.csv` para incluir los meses de publicación.
   - Genera pares clave-valor en el formato:

     ```bash
     <signo_zodiacal>\t<mes_publicación>\t<conteo>
     ```

4. **`reducer2.py`**:
   - Agrupa los datos del Mapper 2 para calcular cuántos usuarios, por signo zodiacal, interactúan con animes publicados en un mes específico.
   - Agrega una leyenda explicativa antes de los resultados.
   - Genera resultados en el formato:

     ```bash
     <signo_zodiacal>\t<mes_publicación>\t<conteo>
     ```

5. **`clean.sh`**:
   - Automatiza la limpieza de directorios previos (`output/` y `output2/`) antes de la ejecución de los MapReduce.
   - Contenido:

     ```bash
     #!/bin/bash
     rm -r output/* output2/* 2>/dev/null
     ```

6. **`run.sh`**:
   - Automatiza la ejecución de ambas fases del trabajo MapReduce (Mapper 1 + Reducer 1 y Mapper 2 + Reducer 2).
   - Configura rutas, valida la existencia de scripts y directorios, y ejecuta ambos trabajos.
   - Fragmento:

     ```bash
     echo "Ejecutando el primer trabajo de MapReduce..."
     $HADOOP_BIN jar $HADOOP_STREAMING_JAR \
         -input $INPUT_FILE \
         -output $OUTPUT_DIR \
         -mapper "python3 $MAPPER1" \
         -reducer "python3 $REDUCER1" \
         -file $MAPPER1 \
         -file $REDUCER1
     ```

7. **Dataset**:
   - **`vshort-users-details-2023.csv`**:
     Contiene información sobre los usuarios, incluyendo su cumpleaños y otras estadísticas relacionadas con el consumo de anime.
   - **`vshort-anime-filtered.csv`**:
     Proporciona información sobre los animes, incluyendo géneros y meses de publicación.

---

## **Ejecución**

1. Asegúrate de que los archivos `vshort-users-details-2023.csv` y `vshort-anime-filtered.csv` estén ubicados en la carpeta `test/` dentro del directorio principal del proyecto.
2. Asegúrate de que los directorios `output/` y `output2/` estén vacíos o que se ejecuten los comandos de limpieza.

---

## **Comandos para Ejecución**

### **1. Limpiar resultados previos**

   ```bash
   ./clean.sh
   ```

### **2. Ejecutar el trabajo completo**

   ```bash
   ./run.sh
   ```

---

## **Estructura de Carpetas**

El proyecto debe tener la siguiente estructura:

```bash
CI5312_BigData/
├── test/
│   ├── vshort-users-details-2023.csv
│   ├── vshort-anime-filtered.csv
├── output/
├── output2/
├── Problema_4/
│   ├── mapper1.py
│   ├── reducer1.py
│   ├── mapper2.py
│   ├── reducer2.py
│   ├── clean.sh
│   ├── run.sh
```

---

## **Resultados**

### **Ejemplo de Salida**

#### **Salida Reducer 2: Relación Signos Zodiacales y Meses de Publicación**

```bash
Relación entre signos zodiacales y meses de publicación
-----------------------------------------------------
Formato:
Signo Zodiacal Mes Publicación Conteo
(Mes Publicación: 1 = Enero, ..., 12 = Diciembre)
-----------------------------------------------------
Aquarius 1 6
Aquarius 2 8
Aries 3 15
Pisces 3 12
Capricorn 12 9
...
```

---

## **Interpretación**

1. Los géneros de anime consumidos varían significativamente según los signos zodiacales. Por ejemplo, **Aquarius** muestra preferencia por animes de acción publicados en enero y febrero.
2. Los datos indican que los estudios pueden estar publicando animes en ciertos meses para atraer audiencias específicas basadas en su signo zodiacal.
