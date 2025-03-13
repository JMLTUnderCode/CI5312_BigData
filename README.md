# CI5312_BigData
Desarrollo de soluciones a un DataSet mediante MapReduce

Link directo a Data Set: [Anime Dataset 2023](https://www.kaggle.com/datasets/dbdmobile/myanimelist-dataset)

## Indicaciones previas

### By Junior Miguel Lara Torres

Estoy usando Python/Pandas para el manejo del dataset como un dataframe y que sea mas sencillo.
Pasos a seguir para trabajar en ellos

1. Crear un envirotment de Python
	```
	python3 -m venv <mi-entorno>         # Ejemplo: python3 -m venv env
	```
	Activar el entorno virtual
	```
	source <mi-entorno>/bin/activate         # Ejemplo: source env/bin/activate
	```
	En caso de querer salir del entorno virtual realiza
	```
	deactivate
	```
	En caso de querer eliminar el entorno virtual
	```
	sudo rm -rf mi_entorno         # Ejemplo sudo rm -rf env
	```
2. Instalar las librerías necesarias
	El repositorio tiene un archivo `requeriments.txt` donde estan todas las dependencias usadas.
	Debes intalarla en tu entorno. 
	```
	pip install -r requirements.txt
	```
	
Documentacion adicional:

1. Para revisar el nombre e informacion de los archivos en el data set de Anime 2023
	Debes tener el archivo `kaggle.json` que contiene la configuracion de la API de Kaggle de tu usuario, esto
	dentro de la carpeta `.kaggle` en `/home/user`.
	```
	kaggle datasets files dbdmobile/myanimelist-dataset
	```

## Content

------------------------------------------ "anime-dataset-2023.csv" ----------------------------------------------

* anime_id: Unique ID for each anime.
* Name: The name of the anime in its original language.
* English name: The English name of the anime.
* Other name: Native name or title of the anime(can be in Japanese, Chinese or Korean).
* Score: The score or rating given to the anime.
* Genres: The genres of the anime, separated by commas.
* Synopsis: A brief description or summary of the anime's plot.
* Type: The type of the anime (e.g., TV series, movie, OVA, etc.).
* Episodes: The number of episodes in the anime.
* Aired: The dates when the anime was aired.
* Premiered: The season and year when the anime premiered.
* Status: The status of the anime (e.g., Finished Airing, Currently Airing, etc.).
* Producers: The production companies or producers of the anime.
* Licensors: The licensors of the anime (e.g., streaming platforms).
* Studios: The animation studios that worked on the anime.
* Source: The source material of the anime (e.g., manga, light novel, original).
* Duration: The duration of each episode.
* Rating: The age rating of the anime.
* Rank: The rank of the anime based on popularity or other criteria.
* Popularity: The popularity rank of the anime.
* Favorites: The number of times the anime was marked as a favorite by users.
* Scored By: The number of users who scored the anime.
* Members: The number of members who have added the anime to their list on the platform.
* Image URL: The URL of the anime's image or poster.

--------------------------------------------- "users-details-2023.csv" ------------------------------------------------

* Mal ID: Unique ID for each user.
* Username: The username of the user.
* Gender: The gender of the user.
* Birthday: The birthday of the user (in ISO format).
* Location: The location or country of the user.
* Joined: The date when the user joined the platform (in ISO format).
* Days Watched: The total number of days the user has spent watching anime.
* Mean Score: The average score given by the user to the anime they have watched.
* Watching: The number of anime currently being watched by the user.
* Completed: The number of anime completed by the user.
* On Hold: The number of anime on hold by the user.
* Dropped: The number of anime dropped by the user.
* Plan to Watch: The number of anime the user plans to watch in the future.
* Total Entries: The total number of anime entries in the user's list.
* Rewatched: The number of anime rewatched by the user.
* Episodes Watched: The total number of episodes watched by the user.

---------------------------------------------- "users-score-2023.csv" -------------------------------------------------

* user_id: Unique ID for each user.
* Username: The username of the user.
* anime_id: Unique ID for each anime.
* Anime Title: The title of the anime.
* rating: The rating given by the user to the anime.
