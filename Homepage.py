# -*- coding: utf-8 -*-
"""#1 - Homework"""

## LIBRARIES IMPORT ##
from libraries import *

## PAGE CONFIGURATION ##
st.set_page_config(
    page_title='Movies',
    page_icon=':🎬:',
    layout='wide',
    initial_sidebar_state='expanded'  # Espandi la barra laterale inizialmente, se desiderato
    )
st.title('🎬📽️ CINE-DATA: Dataset Cinematografici 🎞️🎥')

## VISUALIZE DATASET ##
left_column, right_column = st.columns(2)
left_check = left_column.checkbox("IMDB Dataset")
right_check = right_column.checkbox("GENRE Dataset")

### **INSTALLAZIONE PY-SPARK** ###
# Verify the Spark version running on the virtual cluster
sc = SparkContext.getOrCreate()
assert  "3." in sc.version, "Verify that the cluster Spark's version is 3.x"
print("Spark version:", sc.version)

# Creiamo sessione Spark
spark = SparkSession(sc)
print(spark)


### DATASET ###

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


### IMDB DATASET ###

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

#df_imdb_movies.printSchema()

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
columns_to_convert = ["genre","director","stars"]

# Converti la colonna specificata nel tipo di dato desiderato
for column in columns_to_convert:
    df_imdb_movies = df_imdb_movies.withColumn(column, split(col(column), ","))

# Elimina film ripetuti
df_imdb_movies = df_imdb_movies.distinct()

# Verifica lo schema aggiornato
#df_imdb_movies.printSchema()

# Eliminiamo tutti i film con anno NULL (per le successive operazioni)
df_imdb_movies = df_imdb_movies.filter(df_imdb_movies["year"].isNotNull())

# Visualizza il dataset
if left_check:
    left_column.dataframe(df_imdb_movies)

### GENRE DATASET ###

# Definisci una lista contenente tutti i percorsi dei file CSV
file_paths = [
    action_path, adventure_path, animation_path, biography_path,
    comedy_path, drama_path, fantasy_path, history_path,
    horror_path, music_path, mystery_path, romance_path,
    sci_fi_path, sport_path, thriller_path, war_path
]

# Inizializza un DataFrame vuoto
df_genre_movies = None

# Carica ciascun file CSV e uniscilo al DataFrame combinato
for path in file_paths:
    df = spark.read.option("inferSchema", "true").option("header", "true").csv(path)
    if df_genre_movies is None:
        df_genre_movies = df
    else:
        df_genre_movies = df_genre_movies.union(df)

# Verifica lo schema del DataFrame combinato
#df_genre_movies.printSchema()

# Converti durata in minuti per leggerlo come int
hours = regexp_extract(col("run_length"), r"(\d+)h", 1).cast("int")  # Estrae le ore
minutes = regexp_extract(col("run_length"), r"(\d+)min", 1).cast("int")  # Estrae i minuti
df_genre_movies = df_genre_movies.withColumn("run_length", (hours * 60) + minutes)

# Specifica la colonna da convertire e il relativo tipo di dato
column_to_convert = "genres"

# Converti la colonna specificata nel tipo di dato desiderato
df_genre_movies = df_genre_movies.withColumn(column_to_convert, regexp_replace(col(column_to_convert),r" ",""))
df_genre_movies = df_genre_movies.withColumn(column_to_convert, split(col(column_to_convert), ";"))
df_genre_movies = df_genre_movies.withColumn(column_to_convert, array_remove(col(column_to_convert), ""))

# Elimina film ripetuti
df_genre_movies = df_genre_movies.distinct()

# Verifica lo schema aggiornato
#df_genre_movies.printSchema()

# Visualizza il dataset
if right_check:
    right_column.dataframe(df_genre_movies)


### OPERATIONS ###

## FILM COMUNI TRA DATASET ##

df_common_movies = df_imdb_movies.join(df_genre_movies, (df_imdb_movies["title"] == df_genre_movies["name"])
                                         & (df_imdb_movies["year"] == df_genre_movies["year"]))\
                                        .select(df_imdb_movies["title"],df_imdb_movies['year'])\
                                        .distinct()

df_common_movies.show()
print(df_common_movies.count())


## TUTTI I FILM ##

df_all_movies = df_imdb_movies.select('title','year').union(df_genre_movies.select('name','year')).distinct()

df_all_movies.show()
print(df_all_movies.count())


## FILM PIU' APPREZZATI ##

rating_imdb = df_imdb_movies.select("title","year","rating")
rating_imdb = rating_imdb.withColumnRenamed("rating", "rating_imdb")
df_all_movies_voted = df_all_movies.join(rating_imdb, (df_all_movies["title"]==rating_imdb["title"])
                                   & when(df_all_movies["year"].isNotNull(), (df_all_movies["year"]==rating_imdb["year"])), "left_outer")\
                                   .select(df_all_movies["title"],df_all_movies["year"],rating_imdb["rating_imdb"]).distinct()

rating_genre = df_genre_movies.select("name","year","rating")
rating_genre = rating_genre.withColumnRenamed("rating", "rating_genre")
df_all_movies_voted = df_all_movies_voted.join(rating_genre, (df_all_movies_voted["title"]==rating_genre["name"])
                                         &(df_all_movies_voted["year"]==rating_genre["year"]), "left_outer")\
                                         .select(df_all_movies["title"],df_all_movies["year"],\
                                                 rating_imdb["rating_imdb"],rating_genre["rating_genre"]).distinct()

df_all_movies_voted = df_all_movies_voted.withColumn("rating", when(col("rating_imdb").isNotNull()
                                                             & col("rating_genre").isNotNull(),
                                                             round((col("rating_imdb")+col("rating_genre"))/lit(2),1))\
                                                            .otherwise(round(coalesce(col("rating_imdb"), col("rating_genre")), 1)))

df_all_movies_voted = df_all_movies_voted.select("title","year","rating")
top_10 = df_all_movies_voted.orderBy(desc("rating")).limit(10)

top_10.show()


## FILM PEGGIORI ##

flop_10 = df_all_movies_voted.orderBy("rating").limit(10)
flop_10.show()


## FILM VOTATO DA PIU' UTENTI ##

df_voters = df_all_movies.select("*").withColumn("voters",lit(0))

df_voters_imdb = df_imdb_movies.select("title","year","votes")
df_voters_imdb = df_voters_imdb.withColumnRenamed("votes", "voters_imdb")
df_voters = df_voters.join(df_voters_imdb, (df_voters["title"]==df_voters_imdb["title"])
                     &(df_voters["year"]==df_voters_imdb["year"]), "left_outer")\
                     .select(df_voters["title"],df_voters["year"],df_voters_imdb["voters_imdb"]).distinct()

df_voters_genre = df_genre_movies.select("name","year","num_raters")
df_voters_genre = df_voters_genre.groupBy("name","year").avg("num_raters")
df_voters_genre = df_voters_genre.withColumn("voters_genre", col("avg(num_raters)").cast("int"))
df_voters_genre = df_voters_genre.select("name","year","voters_genre")
df_voters = df_voters.join(df_voters_genre, (df_voters["title"]==df_voters_genre["name"])
                     &(df_voters["year"]==df_voters_genre["year"]), "left_outer")\
                     .select(df_voters["title"],df_voters["year"],df_voters_imdb["voters_imdb"],df_voters_genre["voters_genre"])\
                     .distinct()

df_voters = df_voters.withColumn("voters", greatest(col("voters_imdb"),col("voters_genre")))\
                     .select("title","year","voters")

most_voted = df_voters.orderBy(desc("voters")).limit(1)

most_voted.show()

df_voters = df_all_movies.select("*").withColumn("voters",lit(0))

df_voters_imdb = df_imdb_movies.select("title","year","votes")
df_voters_imdb = df_voters_imdb.withColumnRenamed("votes", "voters_imdb")
df_voters = df_voters.join(df_voters_imdb, (df_voters["title"]==df_voters_imdb["title"])
                     &(df_voters["year"]==df_voters_imdb["year"]), "left_outer")\
                     .select(df_voters["title"],df_voters["year"],df_voters_imdb["voters_imdb"]).distinct()

df_voters_genre = df_genre_movies.select("name","year","num_raters")
df_voters_genre = df_voters_genre.groupBy("name","year").avg("num_raters")
df_voters_genre = df_voters_genre.withColumn("voters_genre", col("avg(num_raters)").cast("int"))
df_voters_genre = df_voters_genre.select("name","year","voters_genre")
df_voters = df_voters.join(df_voters_genre, (df_voters["title"]==df_voters_genre["name"])
                     &(df_voters["year"]==df_voters_genre["year"]), "left_outer")\
                     .select(df_voters["title"],df_voters["year"],df_voters_imdb["voters_imdb"],df_voters_genre["voters_genre"])\
                     .distinct()

df_voters = df_voters.withColumn("voters", greatest(col("voters_imdb"),col("voters_genre")))

most_voted = df_voters.select("title","year","voters").select(max("voters")).collect()[0][0]

most_voted = df_voters.select('title','year','voters').where(col('voters')==most_voted)

most_voted.show()


## FILM RECENSITO DA PIU' UTENTI ##

most_reviewed = df_genre_movies.groupBy('name','year').avg('num_reviews')\
                .select('name','year', 'avg(num_reviews)')

most_reviewed = most_reviewed.select("name","year","avg(num_reviews)")\
                   .orderBy(desc("avg(num_reviews)")).limit(1)

most_reviewed.show()


## RATING PONDERATO ##

# A parità di voti, si studia il rating di ogni film
df_ratio = df_all_movies_voted.join(df_voters, (df_all_movies_voted['title']==df_voters['title'])\
                             & (df_all_movies_voted['year']==df_voters['year']))\
                        .select(df_all_movies_voted['title'], df_all_movies_voted['year'], 'voters', 'rating')

df_ratio = df_ratio.withColumn("weighted_rating", round(df_ratio['rating'] * (log10(col("voters"))),1))
best_movies = df_ratio.select('title','year','weighted_rating', 'rating', 'voters').orderBy(desc('weighted_rating')).limit(10)
best_movies.show(truncate=False)


## FILM PER GENERE ##

df_imdb_exploded = df_imdb_movies.withColumn("genre", explode("genre"))

# Ottieni i valori unici della colonna esplosa
df_unique_genre = df_imdb_exploded.select("genre").distinct()

lista_genres = []
for i in range (df_unique_genre.count()):
    lista_genres.append(df_unique_genre.collect()[i][0])

print(lista_genres)

film_x_genre = {}

for elem in lista_genres:
    imdb_genre = df_imdb_movies.filter(array_contains(col("genre"), elem))\
                                .select('title','year','genre').count()
    film_x_genre[elem] = imdb_genre

print(film_x_genre)

df_diff = df_genre_movies.join(df_common_movies, (df_genre_movies["name"]==df_common_movies["title"])
                            & (df_genre_movies["year"]==df_common_movies["year"]), how="left_anti")

for elem in lista_genres:
    genre_genre = df_diff.filter(array_contains(col("genres"), elem))\
                                .select('name','year','genres').count()
    film_x_genre[elem] += genre_genre

print(film_x_genre)


## MEDIA VOTI PER GENERE ##

df_diff_exploded = df_diff.withColumn("genre", explode("genres"))

# uniamo le tabelle exploded
df_exploded = df_imdb_exploded.select('genre', 'rating').union(df_diff_exploded.select('genre', 'rating'))

df_mean_genre = df_exploded.groupBy("genre").avg('rating')\
                            .select('genre', 'avg(rating)')

df_mean_genre = df_mean_genre.withColumn("rating", round(col("avg(rating)"),1)).select("genre","rating")

# Conta il numero di film per genere
film_count_per_genre = df_exploded.groupBy("genre").agg(count('*').alias('film_count'))

# Unisci i due DataFrame
df_mean_genre_with_count = df_mean_genre.join(film_count_per_genre, 'genre', 'inner')

df_mean_genre_with_count.show()


## DURATA MEDIA DI UN FILM ##

df_time = df_genre_movies.select(avg('run_length'))
df_time = df_time.withColumn("run_length", round(col("avg(run_length)"),1)).select("run_length")

# Vediamo che in media un film dura 2h, ovvero 120min
df_time.show()


## DURATA MEDIA PER GENERE ##

df_genre_exploded = df_genre_movies.withColumn("genre", explode("genres"))

df_time_genre = df_genre_exploded.groupBy('genre').avg('run_length').select('genre', 'avg(run_length)')

df_time_genre = df_time_genre.orderBy(desc('avg(run_length)'))

df_time_genre = df_time_genre.withColumn("run_length", round(col("avg(run_length)"),1))\
                             .select("genre","run_length")

df_time_genre.show()


## TOP FILM PER CADENZA DECENNALE ##

df_year = []
for year in range(1910, 2030, 10):
    df_year.append(df_all_movies_voted.filter((col('year')>=year) & (col('year')<(year+10)))\
                                    .orderBy(desc('rating')).limit(5))
    df_year[-1].show(truncate=False)


## QUALE DECENNIO HA DATO I FILM MIGLIORI MEDIAMENTE E IN ASSOLUTO ##

# Aggiungi la colonna 'decennio' al DataFrame
df_10 = df_all_movies_voted.withColumn('decennio', (col('year') / 10).cast("int") * 10)

df_10_mean = df_10.groupBy('decennio').avg('rating')
df_10_mean = df_10_mean.withColumn('rating', round(col('avg(rating)'), 1)).select('decennio', 'rating')
df_10_mean = df_10_mean.orderBy(desc('rating'))
df_10_mean.show()

df_10_max = df_10.groupBy('decennio').max('rating')
df_10_max = df_10_max.withColumn('rating', round(col('max(rating)'), 1)).select('decennio', 'rating')
df_10_max = df_10_max.orderBy(desc('rating'))
df_10_max.show()


## ATTORI CHE HANNO FATTO PIU' FILM ##

df_imdb_exploded_actors = df_imdb_movies.withColumn("actor", explode("stars"))

df_actors = df_imdb_exploded_actors.groupBy("actor").agg(count("*").alias("movies_number"))
df_actors = df_actors.orderBy(desc("movies_number"))
df_stars = df_actors.limit(10)
df_stars.show()


## ATTORI PIU' PRESENTI CHE HANNO FATTO I FILM PIU' APPREZZATI ##

df_most_appreciated = top_10.join(df_imdb_exploded_actors, (top_10["title"]==df_imdb_exploded_actors["title"])
                                 & (top_10["year"]==df_imdb_exploded_actors["year"]))\
                                  .select(top_10["title"], top_10["year"], top_10["rating"],
                                         df_imdb_exploded_actors["actor"])

df_most_appreciated = df_most_appreciated.withColumnRenamed("actor","top_actor")
df_most_appreciated = df_most_appreciated.join(df_actors, df_most_appreciated["top_actor"]==df_actors["actor"])\
                                         .select(df_most_appreciated["title"], df_actors["actor"],
                                                 df_most_appreciated["rating"], df_actors["movies_number"])

df_film_stars = df_most_appreciated.orderBy(desc("movies_number")).limit(10)
df_film_stars.show(truncate=False)


## ATTORI E REGISTI CON PIU' COLLABORAZIONI ##

df_imdb_directors = df_imdb_movies.withColumn("director", col("director")[0])
df_directors_actors = df_imdb_directors.withColumn("actor", explode("stars"))
df_couple = df_directors_actors.groupBy("director","actor").agg(count("*").alias("movies_number"))
df_couple = df_couple.filter(col("actor")!=col("director"))
df_couple = df_couple.orderBy(desc("movies_number")).limit(10)
df_couple.show()


## COMMENTI TOP 10 ##

df_tweet = df_genre_movies.groupBy('name', 'year', 'rating', 'review_url').agg(count('*'))\
                            .select('name', 'year', 'rating', 'review_url')
df_tweet_top_10 = df_tweet.orderBy(desc('rating')).limit(10)
df_tweet_top_10.show()

# Funzione per recuperare la recensione da un URL
def get_review_from_url(url):
    reviews = []
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            comments = soup.find_all('div', class_='text show-more__control')
            for comment in comments:
                reviews.append(comment.text.strip())
            text = ' '.join(reviews)
            text = text.replace(",", "") \
                        .replace(";", "") \
                        .replace(":", "") \
                        .replace("(", "") \
                        .replace(")", "") \
                        .replace("!", "") \
                        .replace("?", "") \
                        .replace(".", "")
            text = text.lower()
            text = text.split()
            return text
        else:
            return "else"
    except:
        return "eccezione"

# Definisce le stop words
englishStopWords = ["and", "to", "the", "into", "on", "of", "by", "or", "in", "a", "with", "that", "she", "it", "i",
                    "you", "he", "we", "they", "her", "his", "its", "this", "that", "at", "as", "for", "not", "so",
                    "do", "is", "was", "are", "have", "has", "an", "my", "-", "but", "be", "film", "movie", "one",
                   "from", "it's", "me", "where"]

top_tweets = df_tweet_top_10.select('name', 'year', 'rating', 'review_url').collect()

df_top_review = None

for tweet in top_tweets:
    url = tweet['review_url']
    text = get_review_from_url(url)
    text = [word for word in text if word not in englishStopWords]
    top_word_dic = {}
    top_word_list = []

    for word in text:
        if word in top_word_list:
            top_word_dic[word] += 1
        else:
            top_word_dic[word] = 1
            top_word_list.append(word)

    # Ordinare il dizionario filtrato per valori in ordine decrescente
    top_word_dic_ordinato = dict(sorted(top_word_dic.items(), key=lambda x: x[1], reverse=True))
    top_five_words = dict(list(top_word_dic_ordinato.items())[:5])

    # Crea un DataFrame Spark con una colonna di tipo MapType
    df_temp = spark.createDataFrame([(tweet['name'], tweet['year'],
                                 tweet['rating'], top_five_words,)],
                               ['name', 'year', 'rating', "top_words"])

    if df_top_review is None:
        df_top_review = df_temp
    else:
        df_top_review = df_top_review.union(df_temp)

# Mostra il DataFrame
df_top_review.show(truncate=False)


## COMMENTI FLOP 10 ## 

df_tweet_flop_10 = df_tweet.orderBy(('rating')).limit(10)
df_tweet_flop_10.show()

flop_tweets = df_tweet_flop_10.select('name', 'year', 'rating', 'review_url').collect()

df_flop_review = None

for tweet in flop_tweets:
    url = tweet['review_url']
    text = get_review_from_url(url)
    text = [word for word in text if word not in englishStopWords]
    flop_word_dic = {}
    flop_word_list = []

    for word in text:
        if word in flop_word_list:
            flop_word_dic[word] += 1
        else:
            flop_word_dic[word] = 1
            flop_word_list.append(word)

    # Ordinare il dizionario filtrato per valori in ordine decrescente
    flop_word_dic_ordinato = dict(sorted(flop_word_dic.items(), key=lambda x: x[1], reverse=True))
    flop_five_words = dict(list(flop_word_dic_ordinato.items())[:5])

    # Crea un DataFrame Spark con una colonna di tipo MapType
    df_temp = spark.createDataFrame([(tweet['name'], tweet['year'],
                                 tweet['rating'], flop_five_words,)],
                               ['name', 'year', 'rating', "top_words"])

    if df_flop_review is None:
        df_flop_review = df_temp
    else:
        df_flop_review = df_flop_review.union(df_temp)

# Mostra il DataFrame
df_flop_review.show(truncate=False)