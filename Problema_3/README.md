# Problema 3

> [!IMPORTANT]
> Determinar la distribución de hombres y mujeres en los géneros de anime, para así:
> * Item 1: Ver el comportamiento de la popularidad en función de la distribución.
> * Item 2: Ver la tendencia de géneros de anime en función de la distribución.
> * Item 3: Ver si existe relación acerca de las preferencias de la distribución con respecto al origen (source) de los animes que consumen.
> * Item 4: Ver si es posible detectar un patrón referente a qué estudio es más propenso a sacar los tipos de animes que están en estas características.

## Paso 1: Ejecutar el trabajo MapReduce para determinar la distribución de hombres y mujeres en los géneros de anime

Dentro de la carpeta de hadoop de juguete deben existir los siguientes directorios
- `test`: Contendrá todos los archivos vshort del dataset.
- `output`: Contendrá todas las salidas realizadas.
- `MapReduce`: Contendrá todos los archivos de MapReduce en subcarpetas como sigue:
    - `p3-1`: Contendrá el mapper y reducer 1, del problema 3.
    - `p3-2`: Contendrá el mapper y reducer 2, del problema 3.

Una vez estando dentro de la carpeta hadoop de jueguete, ejecutar:

### Para el MapReduce 1

```sh
./bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar -input test/vshort-final_animedataset.csv -output output/output_p3_step1 -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file MapReduce/p3-1/mapper.py -file MapReduce/p3-1/reducer.py
```
### Para el MapReduce 2

```sh
./bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar -input test/vshort-final_animedataset.csv -output output/output_p3_step2 -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file MapReduce/p3-2/mapper.py -file MapReduce/p3-2/reducer.py
```