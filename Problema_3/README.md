# Problema 3

> [!IMPORTANT]
> Determinar la distribución de hombres y mujeres, para así:
>
> * Item 1: Ver el comportamiento de la popularidad en función de la distribución.
> * Item 2: Ver la tendencia de géneros de anime en función de la distribución.
> * Item 3: Ver si existe relación acerca de las preferencias de la distribución con respecto al origen (source) de los animes que consumen.
> * Item 4: Ver si es posible detectar un patrón referente a qué estudio es más propenso a sacar los tipos de animes que están en estas características.

## Indice

- [Problema 3](#problema-3)
	- [Indice](#indice)
	- [Ejecución](#ejecución)
	- [Paso 1: Ejecutar el trabajo MapReduce para determinar la distribución de hombres y mujeres en un anime](#paso-1-ejecutar-el-trabajo-mapreduce-para-determinar-la-distribución-de-hombres-y-mujeres-en-un-anime)
	- [Paso 2: Ejecutar el trabajo MapReduce para Calcular el porcentaje de hombres y mujeres por género de anime](#paso-2-ejecutar-el-trabajo-mapreduce-para-calcular-el-porcentaje-de-hombres-y-mujeres-por-género-de-anime)

## Ejecución

Dentro de la carpeta de hadoop de juguete deben existir los siguientes directorios

* `test`: Contendrá todos los archivos `vshort-*` del dataset.
* `output`: Contendrá todas las salidas realizadas.
* `piplib`: Donde deben estar las librerias a usar y pasar al job. Se debe instalar las siguientes librerias:
  * pycountry

 ```bash
 pip install pycountry -t ./piplib
 ```

 NOTA: Quizas se vayan agregando mas librerias. Revisar este archivo en caso de tener errores de modulos en sus ejecuciones.

* `MapReduce`: Contendrá todos los archivos de MapReduce en subcarpetas como sigue:
  * `p3-1`: Contendrá el mapper y/o reducer del Item 1.
  * `p3-2`: Contendrá el mapper y/o reducer del Item 2.
  * `p3-3`: Contendrá el mapper y/o reducer del Item 3.
  * `p3-4`: Contendrá el mapper y/o reducer del Item 4.

>[!IMPORTANT]
>Estando en la carpeta hadoop.
A continuacion se muestran los pasos y comandos a ejecutar para tener la solución al tercer problema.
> [!NOTE]
> Revisar los path relativos en los comandos. Los comandos mostrados a continuacion funcionan bajo el sistema de archivos de `Junior Miguel Lara Torres`.

## Paso 1: Ejecutar el trabajo MapReduce para determinar la distribución de hombres y mujeres en un anime

* Limpiamos los directorios de salida y copiamos los archivos del trabajo a la carpeta `hadoop/MapReduce/p3-1`

```bash
rm -r output/p3-item1 MapReduce/p3-1/*; cp ../CI5312_BigData/Problema_3/Item_1/* MapReduce/p3-1
```

* Ejecutar el primer trabajo que solventa el primer item.

```bash
./bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar -input test/vshort-final_animedataset.csv -output output/p3-item1 -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file MapReduce/p3-1/mapper.py -file MapReduce/p3-1/reducer.py
```

* En caso de querer observar la salida del Job

```bash
cat output/p3-item1/part-00000
```

## Paso 2: Ejecutar el trabajo MapReduce para Calcular el porcentaje de hombres y mujeres por género de anime

* Limpiamos los directorios de salida y copiamos los archivos del primer sub-trabajo a la carpeta `hadoop/MapReduce/p3-2`

```bash
rm -r output/p3-item2 MapReduce/p3-2/*; cp ../CI5312_BigData/Problema_3/Item_2/* MapReduce/p3-2
```

* Ejecutar el primer sub-trabajo que solventa el segundo item.

```bash
./bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar -input test/vshort-final_animedataset.csv -output output/p3-item2 -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file MapReduce/p3-2/mapper.py -file MapReduce/p3-2/reducer.py
```

* En caso de querer observar la salida del Job

```bash
cat output/p3-item2/part-00000
```
