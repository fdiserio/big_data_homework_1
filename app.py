import os
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import col, count
from pyspark.sql.types import ArrayType, MapType, StringType
from pyspark.sql.functions import *
from pyspark.sql.functions import split
from pyspark.sql.functions import avg
from pyspark.sql.functions import when, col, lit

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

# Verify the Spark version running on the virtual cluster
from pyspark.context import SparkContext
sc = SparkContext.getOrCreate()
 
assert  "3." in sc.version, "Verify that the cluster Spark's version is 3.x"

# Creiamo sessione Spark
spark = SparkSession(sc)
print(spark)

st.title("IMDB DATASET")

# PATH
genres_path = './genres'
imdb_path = 'imdb/movies.csv'
imdb_path_2 = './imdb/imdb_movies.csv'

action_path = genres_path + "/Action.csv"
adventure_path = genres_path + "/Adventure.csv"
animation_path = genres_path + "/Animation.csv"
biography_path = genres_path + "/Biography.csv"
comedy_path = genres_path + "/Comedy.csv"
drama_path = genres_path + "/Drama.csv"
fantasy_path = genres_path + "/Fantasy.csv"
history_path = genres_path + "/History.csv"
horror_path = genres_path + "/Horror.csv"
music_path = genres_path + "/Music.csv"
mystery_path = genres_path + "/Mystery.csv"
romance_path = genres_path + "/Romance.csv"
sci_fi_path = genres_path + "/Sci-Fi.csv"
sport_path = genres_path + "/Sport.csv"
thriller_path = genres_path + "/Thriller.csv"
war_path = genres_path + "/War.csv"

# Apri il file CSV in modalità lettura
with open(imdb_path, 'r', encoding="utf-8") as file:
    # Leggi tutte le righe del file
    lines = file.readlines()
 
# Sostituisci tutte le occorrenze di "" con ''
lines = [line.replace('""', "'") for line in lines]
lines = [line.replace("['", "") for line in lines]
lines = [line.replace("']", "") for line in lines]
lines = [line.replace("',", ",") for line in lines]
lines = [line.replace(", ", ",") for line in lines]
lines = [line.replace(",'", ",") for line in lines]
 
# Apri il file CSV in modalità scrittura e scrivi le righe modificate
with open(imdb_path_2, 'w', encoding="utf-8") as file:
    file.writelines(lines)
    
# IMDB DATASET
df_imdb_movies = spark.read \
                    .option("inferSchema", "true") \
                    .option("header", "true") \
                    .csv(imdb_path_2)

# Lista delle colonne da convertire e il relativo tipo di dato
columns_to_convert = {
    "year": "integer",
    "rating": "float",
    "metascore": "float",
    "votes": "integer",
    "gross": "float"
}

# Converti le colonne specificate nel tipo di dato desiderato
for col_name, col_type in columns_to_convert.items():
    df_imdb_movies = df_imdb_movies.withColumn(col_name, col(col_name).cast(col_type))

    
# Specifica la colonna da convertire e il relativo tipo di dato
columns_to_convert = ["genre","director", "stars"]

# Converti la colonna specificata nel tipo di dato desiderato
for column in columns_to_convert:
    df_imdb_movies = df_imdb_movies.withColumn(column, split(col(column), ","))

# Elimina film ripetuti
df_imdb_movies = df_imdb_movies.distinct()

st.dataframe(df_imdb_movies)

st.dataframe(df_imdb_movies.select("*").where(col("title")=="Barfi!"))

st.title("GENRES DATASET")

# GENRE DATASET

# Definisci una lista contenente tutti i percorsi dei file CSV
file_paths = [
    action_path, adventure_path, animation_path, biography_path,
    comedy_path, drama_path, fantasy_path, history_path,
    horror_path, music_path, mystery_path, romance_path,
    sci_fi_path, sport_path, thriller_path, war_path
]

# Inizializza un DataFrame vuoto
df_all_genre = None

# Carica ciascun file CSV e uniscilo al DataFrame combinato
for path in file_paths:
    df = spark.read.option("inferSchema", "true").option("header", "true").csv(path)
    if df_all_genre is None:
        df_all_genre = df
    else:
        df_all_genre = df_all_genre.union(df)

# Estrai le ore e i minuti utilizzando regexp_extract
hours = regexp_extract(col("run_length"), r"(\d+)h", 1).cast("int")  # Estrae le ore
minutes = regexp_extract(col("run_length"), r"(\d+)min", 1).cast("int")  # Estrae i minuti

# Aggiungi una nuova colonna con il totale in secondi
df_all_genre = df_all_genre.withColumn("run_length", (hours * 60) + minutes)
   
# Specifica la colonna da convertire e il relativo tipo di dato
column_to_convert = "genres"

# Converti la colonna specificata nel tipo di dato desiderato
df_all_genre = df_all_genre.withColumn(column_to_convert, regexp_replace(col(column_to_convert),r" ",""))
df_all_genre = df_all_genre.withColumn(column_to_convert, split(col(column_to_convert), ";"))
df_all_genre = df_all_genre.withColumn(column_to_convert, array_remove(col(column_to_convert), ""))

# Elimina film ripetuti
df_all_genre = df_all_genre.distinct()

st.dataframe(df_all_genre)

st.dataframe(df_all_genre.select("genres").where(col("name")=="Deadpool"))