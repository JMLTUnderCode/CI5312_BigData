# Problema 1

>[!IMPORTANT]
> Analizar la distribución geográfica de los animes y sus valoraciones:
> * Item 1: Ver cuáles países son los que tienen más usuarios que ven anime.
> * Item 2: Determinar si en dichos países las puntuaciones tienden a ser muy altas, bajas o equilibradas.
> * Item 3: Ver cuáles países tienen a las personas que dan muchas puntuaciones positivas y negativas.

# Indice
- [Problema 1](#problema-1)
- [Indice](#indice)
- [Ejecución](#ejecución)
	- [Paso 1: Ejecutar el primer trabajo MapReduce para combinar datos de usuarios y paises.](#paso-1-ejecutar-el-primer-trabajo-mapreduce-para-combinar-datos-de-usuarios-y-paises)
	- [Paso 2: Ejecutar el segundo trabajo MapReduce para combinar datos de usuarios y sus puntuaciones](#paso-2-ejecutar-el-segundo-trabajo-mapreduce-para-combinar-datos-de-usuarios-y-sus-puntuaciones)
		- [Primer Job](#primer-job)
		- [Segundo Job](#segundo-job)
	- [Paso 3: Ejecutar el tercer trabajo MapReduce para combinar datos de ubicaciones y puntuaciones](#paso-3-ejecutar-el-tercer-trabajo-mapreduce-para-combinar-datos-de-ubicaciones-y-puntuaciones)

# Ejecución

Dentro de la carpeta de hadoop de jueguete debe exister los siguientes directorios:
- `test`: Contendrá todos los archivos `vshort-*` del dataset.
- `output`: Contendrá todas las salidas realizadas.
- `piplib`: Donde deben estar las librerias a usar y pasar al job. Se debe instalar las siguientes librerias:
  - pycountry
	```bash
	pip install pycountry -t ./piplib
	```
	NOTA: Quizas se vayan agregando mas librerias. Revisar este archivo en caso de tener errores de modulos en sus ejecuciones.
- `MapReduce`: Contendrá todos los archivos de MapReduce en subcarpetas como sigue:
  - `p1-1`: Contendrá el mapper y/o reducer del Item 1.
  - `p1-2`: Contendrá el mapper y/o reducer del Item 2.
  - `p1-3`: Contendrá el mapper y/o reducer del Item 3.

>[!IMPORTANT]
> El sistema de archivos que se espera trabajar es:
> ```bash
>    hadoop/
>    ├── <archivos en general de hadoop>/	
>    ├── test/
>        ├── *.csv
>    ├── output/
>        ├── p1-item1/
>            ├── part-00000
>            ├── part-00001
>        ├── p1-item2/
>            ├── part-00000
>        ├── p*-item*/
>            ├── part-*
>        ├── ...
>    ├── MapReduce/
>        ├── p1-1/
>            ├── mapper.py
>            ├── reducer.py
>        ├── p1-2/
>            ├── mapper.py
>            ├── reducer.py
>        ├── p2-*/
>            ├── *.py
>    ├── piplib/
>        ├── *lib.py
> ```

>[!IMPORTANT]
>Estando en la carpeta hadoop.
A continuacion se muestran los pasos y comandos a ejecutar para tener la solución al primer problema.

> [!NOTE]
> Revisar los path relativos en los comandos. Los comandos mostrados a continuacion funcionan bajo el sistema de archivos de `Junior Miguel Lara Torres`.

## Paso 1: Ejecutar el primer trabajo MapReduce para combinar datos de usuarios y paises.

* Limpiamos los directorios de salida y copiamos los archivos del trabajo a la carpeta `hadoop/MapReduce/1`
```bash
rm -r output/p1-item1 MapReduce/p1-1/*; cp ../CI5312_BigData/Problema_1/Item_1/* MapReduce/p1-1 
```

* Ejecutar el primer trabajo que solventa el primer item.
```bash
./bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar -input test/vshort-users-details-2023.csv -output output/p1_item1 -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file MapReduce/p1-1/mapper.py -file MapReduce/p1-1/reducer.py -file piplib
```

* En caso de querer observar la salida del Job
```bash
cat output/p1-item1/part-00000
```

## Paso 2: Ejecutar el segundo trabajo MapReduce para combinar datos de usuarios y sus puntuaciones

>[!NOTE]
> Este item se divide el dos subjobs.

### Primer Job

* Limpiamos los directorios de salida y copiamos los archivos del primer sub-trabajo a la carpeta `hadoop/MapReduce/p1-2`
```bash
rm -r output/p1-item2-part1 MapReduce/p1-2/*; cp ../CI5312_BigData/Problema_1/Item_2/MapReduce1/* MapReduce/p1-2
```

* Ejecutar el primer sub-trabajo que solventa el segundo item.
```bash
./bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar -input test/vshort-users-score-2023.csv -output output/p1-item2-part1 -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file MapReduce/p1-2/mapper.py -file MapReduce/p1-2/reducer.py
```

* En caso de querer observar la salida del Job
```bash
cat output/p1-item2-part1/part-00000
```

### Segundo Job

>[!IMPORTANT]
> Este job recibe dos archivos de salidas de otros jobs, por tanto es natural dudar con el tema de lectura de archivo y el streaming de lo mismos. 
>
> Recordemos hay particiones de un archivo de salida en job de Hadoop (Ej. `part-00000`, `part-00001`, ...) y por lo tanto es necesario especificar la carpeta de salida para que el job pueda leer todas las partes.
> 
> Para el caso de streaming simplemente se debe pasar la dirección de la carpeta donde esta la salida del job previo. En este ejemplo es `-input output/output_item1` y al parecer a nivel de streaming no hay problema.
> 
>  Ahora bien en caso de querer, no por streaming sino lectura de archivo como tal, se debe implementar una lógica de lectura basado en esta idea de partes de un archivo, para mas información de código ir al `mapper.py` de este segundo sub-job función llamada `load_user_averages`.

* Limpiamos los directorios de salida y copiamos los archivos del segundo sub-trabajo a la carpeta `hadoop/MapReduce/2`
```bash
rm -r output/p1-item2-part2 MapReduce/p1-2/*; cp ../CI5312_BigData/Problema_1/Item_2/MapReduce2/* MapReduce/p1-2
```

* Ejecutar el segundo sub-trabajo que solventa el segundo item.
```bash
./bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar -input output/p1-item1 -output output/p1-item2_part2 -mapper "python3 mapper.py" -file MapReduce/2/mapper.py -file output/p1-item2-part1
```

* En caso de querer observar la salida del Job
```bash
cat output/p1-item2-part2/part-00000
```

## Paso 3: Ejecutar el tercer trabajo MapReduce para combinar datos de ubicaciones y puntuaciones

>[!NOTE]
> PARA QUIEN HAGA ESTE PARTE: Leer esta nota y luego borrarla de haberla entendido. La idea de este item 3 es ordenar la salida del item 2. Ya el item do, tal como dice la documentacion del codigo reducer.py asociado al item 2 se retorna `country - count - average-total` por tanto, es cuestion de manipular la salida `reducer.py` para que salga ordenada.