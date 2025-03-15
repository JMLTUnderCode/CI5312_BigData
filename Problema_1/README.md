# Problema 1

Analizar la distribución geográfica de los animes y sus valoraciones:
* Ver cuáles países son los que tienen más usuarios que ven anime
* Determinar si en dichos países las puntuaciones tienden a ser muy altas, bajas o equilibradas.
* Ver cuáles países tienen a las personas que dan muchas puntuaciones positivas y negativas.

## Paso 1: Ejecutar el primer trabajo MapReduce para combinar datos de usuarios y ubicaciones

Dentro de la carpeta de hadoop de jueguete debe exister los siguientes directorios:
- `test`: Contendrá todos los archivos vshort del dataset.
- `output`: Contendrá todas las salidas realizadas.
- `MapReduce`: Contendrá todos los archivos de MapReduce en subcarpetas como sigue:
  - `1`: Contendrá el mapper y reducer 1.
  - `2`: Contendrá el mapper y reducer 2.
  - `3`: Contendrá el mapper y reducer 3.
  - Asi sucesivamente.

Una vez estando dentro de la carpeta hadoop de jueguete.
```
./bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar -input test/vshort-users-details-2023.csv -output output/output_step1 -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file MapReduce/1/mapper.py -file MapReduce/1/reducer.py
```

## Paso 2: Ejecutar el segundo trabajo MapReduce para combinar datos de usuarios y sus puntuaciones

Estando dentro de la carpeta hadoop de juguete, ejecutar:
```
./bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar -input test/vshort-users-score-2023.csv -output output/output_step2 -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file MapReduce/2/mapper.py -file MapReduce/2/reducer.py
```

## Paso 3: Ejecutar el tercer trabajo MapReduce para combinar datos de ubicaciones y puntuaciones

Falta documentar