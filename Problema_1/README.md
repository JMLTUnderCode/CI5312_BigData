# Problema 1

Analizar la distribución geográfica de los animes y sus valoraciones:
* Ver cuáles países son los que tienen más usuarios que ven anime
* Determinar si en dichos países las puntuaciones tienden a ser muy altas, bajas o equilibradas.
* Ver cuáles países tienen a las personas que dan muchas puntuaciones positivas y negativas.

## Paso 1: Ejecutar el primer trabajo MapReduce para combinar datos de usuarios y ubicaciones

Dentro de la carpeta de hadoop de jueguete debe exister los siguientes directorios:
- `test`: Contendrá todos los archivos vshort del dataset.
- `output`: Contendrá todas las salidas realizadas.
- `piplib`: Donde debe estar las librerias a usar y pasar al job. Se debe instalar las siguientes librerias:
  - pycountry
	```
	pip install pycountry -t ./piplib
	```
	NOTA: Se podran ir agregando mas. Revisar este archivo en caso de tener errores de modulos en sus codigos.
- `MapReduce`: Contendrá todos los archivos de MapReduce en subcarpetas como sigue:
  - `1`: Contendrá el mapper y reducer 1.
  - `2`: Contendrá el mapper y reducer 2.
  - `3`: Contendrá el mapper y reducer 3.
  - Asi sucesivamente.

Una vez estando dentro de la carpeta hadoop de jueguete.
```
rmdir output/output_step1
```
```
rm -r output/output_step1 MapReduce/1/*; cp ../CI5312_BigData/Problema_1/MapReduce1/* MapReduce/1 
rm -r output/output_step2 MapReduce/2/*; cp ../CI5312_BigData/Problema_1/MapReduce2/* MapReduce/2 
rm -r output/output_step3 MapReduce/3/*; cp ../CI5312_BigData/Problema_1/MapReduce3/* MapReduce/3
```
```
./bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar -input test/vshort-users-details-2023.csv -output output/output_step1 -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file MapReduce/1/mapper.py -file MapReduce/1/reducer.py -file piplib
```

## Paso 2: Ejecutar el segundo trabajo MapReduce para combinar datos de usuarios y sus puntuaciones

Estando dentro de la carpeta hadoop de juguete, ejecutar:
```
./bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar -input test/vshort-users-score-2023.csv -output output/output_step2 -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file MapReduce/2/mapper.py -file MapReduce/2/reducer.py
```

## Paso 3: Ejecutar el tercer trabajo MapReduce para combinar datos de ubicaciones y puntuaciones

Falta documentar