# -*- coding: utf-8 -*-
"""#3 - Homework - LoadData"""

## LIBRARIES IMPORT ##
from libraries import *

# CONNESSIONE AL CLUSTER
uri_D = "mongodb+srv://Homework3:Homework3@clusterhomework3.1gev4ck.mongodb.net/?retryWrites=true&w=majority&appName=ClusterHomework3"
uri_F = "mongodb+srv://Homework3:Homework3@homework3.fc8huhi.mongodb.net/?retryWrites=true&w=majority&appName=Homework3"
uri = uri_F

# Create a new client and connect to the server
client = MongoClient(uri)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


### DATASET ###
db = client['Homework3']
collection_imdb = db['imdb']        # Dataset imdb
collection_genre = db['genre']      # Dataset genre
collection_union = db["Data_NoSQL"] # Unione dei due Dataset


### PATH ###
imdb_path = 'imdb/movies.csv'
imdb_path_2 = './imdb/imdb_movies.csv'
genres_path = './genres'

action_path = genres_path + "/Action.csv"
adventure_path = genres_path + "/Adventure.csv"
animation_path = genres_path + "/Animation.csv"
biography_path = genres_path + "/Biography.csv"
comedy_path = genres_path + "/Comedy.csv"
crime_path = genres_path + "/Crime.csv"
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


### FUNZIONE DI RIMOZIONE DUPLICATI ###
def remove_duplicates(collection):
    # Recupera un documento per ottenere i campi dinamicamente
    sample_document = collection.find_one()
    if sample_document is None:
        print("La collection è vuota.")
        return
 
    # Crea il campo _id per il gruppo basato su tutti i campi del documento
    group_id = {field: f"${field}" for field in sample_document if field != "_id"}
 
    # Aggrega i documenti per trovare i duplicati usando tutti i campi eccetto _id
    pipeline = [
        {
            "$group": {
                "_id": group_id,
                "unique_ids": {"$addToSet": "$_id"},
                "count": {"$sum": 1}
            }
        },
        {
            "$match": {
                "count": {"$gt": 1}
            }
        }
    ]
 
    duplicates = list(collection.aggregate(pipeline))
 
    # Rimuove i duplicati mantenendo solo il primo documento inserito
    for doc in duplicates:
        ids_to_remove = doc["unique_ids"][1:]  # Mantiene solo il primo documento
        collection.delete_many({"_id": {"$in": ids_to_remove}})
 
    print("Duplicati rimossi con successo!")


### INSERIMENTO DATASET IMDB ###
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
    
# Leggere il file CSV
df_imdb = pd.read_csv(imdb_path_2)

# Converti la colonna in numerica, impostando errors='coerce' per convertire i valori non numerici in NaN
df_imdb['year'] = pd.to_numeric(df_imdb['year'], errors='coerce')
 
# Rimuovi le righe che contengono NaN
df_imdb = df_imdb.dropna(subset=['year'])
 
# Converti di nuovo la colonna in interi
df_imdb['year'] = df_imdb['year'].astype(int)

# Convertire il DataFrame in una lista di dizionari
documents_imdb = df_imdb.to_dict(orient='records')

# Inserire i documenti nella collezione MongoDB
result = collection_imdb.insert_many(documents_imdb)

# Rimuove i duplicati nelle collection 'imdb'
remove_duplicates(collection_imdb)

# Stampa stringa di fine creazione e conteggio
print("Creata collection imdb")
count_imdb = list(collection_imdb.aggregate([{'$count': 'count'}]))[0]['count']
print(f"Ci sono {count_imdb} film nella collection imdb")
    
    
### INSERIMENTO DATASET GENRE ###
# Definisci una lista contenente tutti i percorsi dei file CSV
file_paths = [
    action_path, adventure_path, animation_path, biography_path,
    comedy_path, crime_path, drama_path, fantasy_path, history_path,
    horror_path, music_path, mystery_path, romance_path,
    sci_fi_path, sport_path, thriller_path, war_path
]

# Inizializza un DataFrame vuoto
df_genre_movies = pd.DataFrame()

# Carica ciascun file CSV e uniscilo al DataFrame combinato
for path in file_paths:
    df = pd.read_csv(path)
    df_genre_movies = pd.concat([df_genre_movies, df], ignore_index=True)

# Converti durata in minuti per leggerlo come int
# Estrai le ore e i minuti utilizzando le espressioni regolari
hours = df_genre_movies['run_length'].str.extract(r'(\d+)h', expand=False).astype(float)
minutes = df_genre_movies['run_length'].str.extract(r'(\d+)min', expand=False).astype(float)
 
# Calcola la durata totale in minuti
df_genre_movies['run_length'] = hours.fillna(0) * 60 + minutes.fillna(0)

# Sostituisci "; " con "," nella colonna "genres"
df_genre_movies['genres'] = df_genre_movies['genres'].str.replace('; ', ',')

# Rimuovi l'ultima virgola dalla colonna 'genres'
df_genre_movies['genres'] = df_genre_movies['genres'].str.rstrip(',')

# Rinomina la colonna 'name' in 'title'
df_genre_movies = df_genre_movies.rename(columns={'name': 'title'})

# Rinomina la colonna 'genres' in 'genre'
df_genre_movies = df_genre_movies.rename(columns={'genres': 'genre'})

# Converte il DataFrame in un formato adatto per MongoDB
data_genre = df_genre_movies.to_dict(orient='records')
 
# Inserisce i nuovi documenti nella collection
collection_genre.insert_many(data_genre)

# Rimuove i duplicati nelle collection 'imdb'
remove_duplicates(collection_genre)

# Stampa stringa di fine creazione e conteggio
print("Creata collection genre")
count_genre = list(collection_genre.aggregate([{'$count': 'count'}]))[0]['count']
print(f"Ci sono {count_genre} film nella collection genre")


### CREAZIONE COLLECTION UNION ###
pipeline_union = [
        {"$unionWith": {"coll": "genre"}}
    ]

results_union = list(collection_imdb.aggregate(pipeline_union))
collection_union.insert_many(results_union)

# Stampa stringa di fine creazione e conteggio
print("Creata unione delle collection")
count_union = list(collection_union.aggregate([{'$count': 'count'}]))[0]['count']
print(f"Ci sono {count_union} film nella collection unione")





'''

### OPERATIONS ###
# Menu' di Checkbox
with st.sidebar:

    spazio = st.columns(3)
    spazio[1].header("$\color{red}\\bf{QUERY}$")
    st.subheader("",divider="rainbow")

    left_column_query, right_column_query = st.columns(2)

    # Lista delle fasi
    queries = [
        "Film Comuni",
        "Tutti i Film",
        "TOP 10",
        "FLOP 10",
        "Film più votato",
        "Film più recensito",
        "Film per genere",
        "Media Voti per genere",
        "Durata Media di un Film",
        "Durata Media di un Film per genere",
        "Rating Ponderato",     
        "Top film per cadenza decennale",   
        "Top Decennio",
        "Mean Decennio",
        "Attori in più film",
        "Attori più presenti nei film migliori",
        "Attori e Registri con più collaborazioni",     
        "Parole più ricorrenti nei commenti della Top 10",
        "Parole più ricorrenti nei commenti della Flop 10"
    ]

    lista_select = [False for i in range(19)]

    for i in range (0,10,2):
        lista_select[i] = left_column_query.checkbox(queries[i])
        lista_select[i+1] = right_column_query.checkbox(queries[i+1])

    lista_select[10] = st.checkbox(queries[10])
    lista_select[11] = st.checkbox(queries[11])

    left_column_query, right_column_query = st.columns(2)

    for i in range (12,16,2):
        lista_select[i] = left_column_query.checkbox(queries[i])
        lista_select[i+1] = right_column_query.checkbox(queries[i+1])

    lista_select[16] = st.checkbox(queries[16])

    left_column_query, right_column_query = st.columns(2)

    lista_select[17] = left_column_query.checkbox(queries[17])
    lista_select[18] = right_column_query.checkbox(queries[18])

st.subheader("",divider="rainbow")

left_query1, right_query1 = st.columns(2)

## FILM COMUNI TRA DATASET ##
@st.cache_resource
def common_film(_df_imdb_movies, _df_genre_movies):
    df_common_movies = df_imdb_movies.join(df_genre_movies, (df_imdb_movies["title"] == df_genre_movies["name"])
                                            & (df_imdb_movies["year"] == df_genre_movies["year"]))\
                                            .select(df_imdb_movies["title"],df_imdb_movies['year'])\
                                            .distinct()
    return df_common_movies

df_common_movies = common_film(df_imdb_movies, df_genre_movies)

if lista_select[0]:
    left_query1.header("$\\bold{Common}$ $\\bold{Film}$")
    left_query1.dataframe(df_common_movies, hide_index=True, column_config={
        "year": st.column_config.NumberColumn(format="%d"),
        })


## TUTTI I FILM ##
@st.cache_resource
def all_film(_df_imdb_movies, _df_genre_movies):
    df_all_movies = df_imdb_movies.select('title','year').union(df_genre_movies.select('name','year')).distinct()
    return df_all_movies

df_all_movies = all_film(df_imdb_movies, df_genre_movies)

if lista_select[1]:
    right_query1.header("$\\bold{All}$ $\\bold{Film}$")
    right_query1.dataframe(df_all_movies, hide_index=True, column_config={
        "year": st.column_config.NumberColumn(format="%d"),
        })


left_query2, right_query2 = st.columns(2)

## FILM PIU' APPREZZATI ##
@st.cache_resource
def best_film(_df_imdb_movies, _df_genre_movies):
    
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

    return top_10, df_all_movies_voted

top_10, df_all_movies_voted = best_film(df_imdb_movies, df_genre_movies)

if lista_select[2]:
    left_query2.header("$\\bold{TOP}$ $\\bold{10}$")
    left_query2.dataframe(top_10, use_container_width=True, hide_index=True, column_config={
        "rating": st.column_config.ProgressColumn(
            min_value=0,
            max_value=10,  
            format="%f",
        ),
        "year": st.column_config.NumberColumn(format="%d")
    })


## FILM PEGGIORI ##
@st.cache_resource
def flop_film(_df_all_movies_voted):
    flop_10 = df_all_movies_voted.orderBy("rating").limit(10)
    return flop_10

flop_10 = flop_film(df_all_movies_voted)

if lista_select[3]:
    right_query2.header("$\\bold{FLOP}$ $\\bold{10}$")
    right_query2.dataframe(flop_10, use_container_width=True, hide_index=True, column_config={
        "rating": st.column_config.ProgressColumn(
            min_value=0,
            max_value=10,  
            format="%f",
        ),
        "year": st.column_config.NumberColumn(format="%d")
    })
    

left_line3, right_line3 = st.columns(2)

## FILM VOTATO DA PIU' UTENTI ##
@st.cache_resource
def most_voted_film(_df_imdb_movies, _df_genre_movies, _df_all_movies):

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

    return most_voted, df_voters

most_voted, df_voters = most_voted_film(df_imdb_movies, df_genre_movies, df_all_movies)
   
if lista_select[4]:
    left_line3.header("$\\bold{Most}$ $\\bold{Voted}$")
    left_line3.dataframe(most_voted, hide_index=True, column_config={
        "year": st.column_config.NumberColumn(format="%d"),
        "voters": st.column_config.NumberColumn(format="%d")
    })


## FILM RECENSITO DA PIU' UTENTI ##
@st.cache_resource
def most_reviewed_film(_df_genre_movies):

    most_reviewed = df_genre_movies.groupBy('name','year').avg('num_reviews')\
                    .select('name','year', 'avg(num_reviews)')

    most_reviewed = most_reviewed.select("name","year","avg(num_reviews)")\
                    .orderBy(desc("avg(num_reviews)")).limit(1)

    return most_reviewed

most_reviewed = most_reviewed_film(df_genre_movies)

if lista_select[5]:
    right_line3.header("$\\bold{Most}$ $\\bold{Reviewed}$")
    right_line3.dataframe(most_reviewed, hide_index=True, column_config={
        "year": st.column_config.NumberColumn(format="%d"),
        "avg(num_reviews)": st.column_config.NumberColumn("reviews", format="%d")
    })


left_line4, right_line4 = st.columns(2)

## FILM PER GENERE ##
@st.cache_resource
def film_x_genre(_df_imdb_movies, _df_genre_movies, _spark):

    df_imdb_exploded = df_imdb_movies.withColumn("genre", explode("genre"))

    # Ottieni i valori unici della colonna esplosa
    df_unique_genre = df_imdb_exploded.select("genre").distinct()

    lista_genres = []
    for i in range (df_unique_genre.count()):
        lista_genres.append(df_unique_genre.collect()[i][0])

    film_x_genre = {}

    for elem in lista_genres:
        imdb_genre = df_imdb_movies.filter(array_contains(col("genre"), elem))\
                                    .select('title','year','genre').count()
        film_x_genre[elem] = imdb_genre


    df_diff = df_genre_movies.join(df_common_movies, (df_genre_movies["name"]==df_common_movies["title"])
                                & (df_genre_movies["year"]==df_common_movies["year"]), how="left_anti")

    for elem in lista_genres:
        genre_genre = df_diff.filter(array_contains(col("genres"), elem))\
                                    .select('name','year','genres').count()
        film_x_genre[elem] += genre_genre

    df = pd.DataFrame(list(film_x_genre.items()), columns=['genre', 'number_of_films'])

    return df, df_diff, df_imdb_exploded

film_x_genre, df_diff, df_imdb_exploded = film_x_genre(df_imdb_movies, df_genre_movies, spark)

if lista_select[6]:
    left_line4.header("$\\bold{Film}$ $\\bold{x}$ $\\bold{Genre}$")
    left_line4.dataframe(film_x_genre, hide_index=True, column_config={
        "number_of_films": st.column_config.NumberColumn(format="%d")
        })


## MEDIA VOTI PER GENERE ##
@st.cache_resource
def mean_genre(_df_diff, _df_imdb_exploded):

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

    return df_mean_genre_with_count

df_mean_genre_with_count = mean_genre(df_diff, df_imdb_exploded)

if lista_select[7]:
    right_line4.header("$\\bold{Votes}$ $\\bold{x}$ $\\bold{Genre}$")
    right_line4.dataframe(df_mean_genre_with_count, hide_index=True, column_config={
        "rating": st.column_config.ProgressColumn(
            min_value=0,
            max_value=10,  
            format="%f",
        ),
        "film_count": st.column_config.NumberColumn(format="%d")
    })


left_line5, right_line5 = st.columns(2)

## DURATA MEDIA DI UN FILM ##
@st.cache_resource
def mean_time(_df_genre_movies):

    df_time = df_genre_movies.select(avg('run_length'))
    df_time = df_time.withColumn("run_length", round(col("avg(run_length)"),1)).select("run_length")

    return df_time

df_time = mean_time(df_genre_movies)

if lista_select[8]:
    left_line5.header("$\\bold{Mean}$ $\\bold{Time}$ \
                      $\\bold{x}$ $\\bold{Film}$")
    left_line5.dataframe(df_time, hide_index=True)


## DURATA MEDIA PER GENERE ##
@st.cache_resource
def mean_time_genre(_df_genre_movies):

    df_genre_exploded = df_genre_movies.withColumn("genre", explode("genres"))

    df_time_genre = df_genre_exploded.groupBy('genre').avg('run_length').select('genre', 'avg(run_length)')

    df_time_genre = df_time_genre.orderBy(desc('avg(run_length)'))

    df_time_genre = df_time_genre.withColumn("run_length", round(col("avg(run_length)"),1))\
                                .select("genre","run_length")

    return df_time_genre

df_time_genre = mean_time_genre(df_genre_movies)

if lista_select[9]:
    right_line5.header("$\\bold{Mean}$ $\\bold{Time}$ \
                        $\\bold{x}$ $\\bold{Genre}$")
    right_line5.dataframe(df_time_genre, hide_index=True)


## RATING PONDERATO ##
@st.cache_resource
def rating_ponderato(_df_all_movies_voted, _df_voters):
    # A parità di voti, si studia il rating di ogni film
    df_ratio = df_all_movies_voted.join(df_voters, (df_all_movies_voted['title']==df_voters['title'])\
                                & (df_all_movies_voted['year']==df_voters['year']))\
                            .select(df_all_movies_voted['title'], df_all_movies_voted['year'], 'voters', 'rating')

    df_ratio = df_ratio.withColumn("weighted_rating", round(df_ratio['rating'] * (log10(col("voters"))),1))
    best_movies = df_ratio.select('title','year','weighted_rating', 'rating', 'voters').orderBy(desc('weighted_rating')).limit(10)

    return best_movies

best_movies = rating_ponderato(df_all_movies_voted, df_voters)

if lista_select[10]:
    st.header("$\\bold{Weighted}$ $\\bold{Rating}$")
    st.dataframe(best_movies, use_container_width=True, hide_index=True, column_config={
        "rating": st.column_config.ProgressColumn(
            min_value=0,
            max_value=10,  
            format="%f",
        ),
        "voters": st.column_config.NumberColumn(format="%d"),
        "year": st.column_config.NumberColumn(format="%d")
    })
    
    st.write("""
            ### Nota:
            Il rating ponderato è una metrica che tiene conto sia del rating di un film che del numero di voti ricevuti.
            La formula usata è la seguente:
        
            `weighted_rating = rating * log10(voters)`
        
            Dove:
            - `rating` è il rating del film
            - `voters` è il numero di voti ricevuti
            - `log10` è il logaritmo in base 10
            """)    


## TOP FILM PER CADENZA DECENNALE ##
@st.cache_resource
def top_decennio(_df_all_movies_voted):
    df_decade = df_all_movies_voted.withColumn("decade", (floor(col("year") / 10) * 10).cast("int"))
    k = 5
    window_spec = Window.partitionBy("decade").orderBy(col("rating").desc())
    df_top_decade = df_decade[['title','year','rating','decade']].withColumn("row_number", row_number().over(window_spec))
    df_top_decade = df_top_decade.filter(col("row_number") <= k).drop("row_number").drop("decade")
    return df_top_decade, df_decade

df_top_decade, df_decade = top_decennio(df_all_movies_voted)

if lista_select[11]:
    st.header("$\\bold{Film}$ $\\bold{x}$ $\\bold{Decade}$")
    st.dataframe(df_top_decade, use_container_width=True, hide_index=True, column_config={
            "year": st.column_config.NumberColumn(format="%d")
        })
    

## QUALE DECENNIO HA DATO I FILM MIGLIORI MEDIAMENTE E IN ASSOLUTO ##
@st.cache_resource
def top_10_decennio(_df_decade):
    df_10_mean = df_decade.groupBy('decade').avg('rating')
    df_10_mean = df_10_mean.withColumn('rating', round(col('avg(rating)'), 1)).select('decade', 'rating')
    df_10_mean = df_10_mean.orderBy(desc('rating'))

    df_10_max = df_decade.groupBy('decade').max('rating')
    df_10_max = df_10_max.withColumn('rating', round(col('max(rating)'), 1)).select('decade', 'rating')
    df_10_max = df_10_max.orderBy(desc('rating'))
    
    return df_10_max, df_10_mean

df_10_max, df_10_mean = top_10_decennio(df_decade)

left_line7, right_line7 = st.columns(2)

if lista_select[12]:
    left_line7.header("$\\bold{TOP}$ $\\bold{x}$ $\\bold{Decade}$")
    left_line7.dataframe(df_10_max, hide_index=True, column_config={
            "decade": st.column_config.NumberColumn(format="%d")
    })

if lista_select[13]:
    right_line7.header("$\\bold{MEAN}$ $\\bold{x}$ $\\bold{Decade}$")
    right_line7.dataframe(df_10_mean, hide_index=True, column_config={
            "decade": st.column_config.NumberColumn(format="%d")
        })


left_line8, right_line8 = st.columns(2)

## ATTORI CHE HANNO FATTO PIU' FILM ##
@st.cache_resource
def actors(_df_imdb_movies):

    df_imdb_exploded_actors = df_imdb_movies.withColumn("actor", explode("stars"))

    df_actors = df_imdb_exploded_actors.groupBy("actor").agg(count("*").alias("movies_number"))

    df_actors = df_actors.orderBy(desc("movies_number"))
    
    df_stars = df_actors.limit(10)

    return df_stars, df_actors, df_imdb_exploded_actors

df_stars, df_actors, df_imdb_exploded_actors = actors(df_imdb_movies)

if lista_select[14]:
    left_line8.header("$\\bold{Stars}$")
    left_line8.dataframe(df_stars, hide_index=True, column_config={
        "movies_number": st.column_config.NumberColumn(format="%d ⭐")
    })


## ATTORI PIU' PRESENTI CHE HANNO FATTO I FILM PIU' APPREZZATI ##
@st.cache_resource
def star_x_film(_top_10, _df_imdb_exploded_actors, _df_actors):

    df_most_appreciated = top_10.join(df_imdb_exploded_actors, (top_10["title"]==df_imdb_exploded_actors["title"])
                                    & (top_10["year"]==df_imdb_exploded_actors["year"]))\
                                    .select(top_10["title"], top_10["year"], top_10["rating"],
                                            df_imdb_exploded_actors["actor"])

    df_most_appreciated = df_most_appreciated.withColumnRenamed("actor","top_actor")
    df_most_appreciated = df_most_appreciated.join(df_actors, df_most_appreciated["top_actor"]==df_actors["actor"])\
                                            .select(df_most_appreciated["title"], df_actors["actor"],
                                                    df_most_appreciated["rating"], df_actors["movies_number"])

    df_film_stars = df_most_appreciated.orderBy(desc("movies_number")).limit(10)

    return df_film_stars

df_stars_film = star_x_film(top_10, df_imdb_exploded_actors, df_actors)

if lista_select[15]:
    right_line8.header("$\\bold{Stars}$ $\\bold{Most}$ $\\bold{Film}$")
    right_line8.dataframe(df_stars_film, hide_index=True)


## ATTORI E REGISTI CON PIU' COLLABORAZIONI ##
@st.cache_resource
def actor_director(_df_imdb_movies):
   
    df_imdb_directors = df_imdb_movies.withColumn("director", col("director")[0])
    df_directors_actors = df_imdb_directors.withColumn("actor", explode("stars"))
    df_couple = df_directors_actors.groupBy("director","actor").agg(count("*").alias("movies_number"))
    df_couple = df_couple.filter(col("actor")!=col("director"))
    df_couple = df_couple.orderBy(desc("movies_number")).limit(10)
 
    return df_couple

df_couple = actor_director(df_imdb_movies)

if lista_select[16]:
    st.header("$\\bold{Actor}$ $\\bold{x}$ $\\bold{Director}$")
    st.dataframe(df_couple, hide_index=True)


left_line9, right_line9 = st.columns(2)

## COMMENTI TOP 10 ##
# Funzione per recuperare la recensione da un URL
@st.cache_data
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

@st.cache_resource
def top_tweet(_df_genre_movies, _spark):
    
    df_tweet = df_genre_movies.groupBy('name', 'year', 'rating', 'review_url').agg(count('*'))\
                                .select('name', 'year', 'rating', 'review_url')
    
    df_tweet_top_10 = df_tweet.orderBy(desc('rating')).limit(10)

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

        # Crea un DataFrame Panda 
        df_temp = pd.DataFrame([(tweet['name'], tweet['year'], tweet['rating'], top_five_words)], 
                               columns=['name', 'year', 'rating', "top_words"])

        if df_top_review is None:
            df_top_review = df_temp
        else:
            df_top_review = pd.concat([df_top_review, df_temp], ignore_index=True)

    return df_top_review, df_tweet

df_top_review, df_tweet = top_tweet(df_genre_movies, spark)

if lista_select[17]:
    left_line9.header("$\\bold{Film}$ $\\bold{Top}$ $\\bold{Tweet}$")
    left_line9.dataframe(df_top_review, hide_index=True, column_config={
            "year": st.column_config.NumberColumn(format="%d")
        })


## COMMENTI FLOP 10 ## 
@st.cache_resource
def flop_tweet(_df_tweet, _spark):
        
    df_tweet_flop_10 = df_tweet.orderBy(('rating')).limit(10)

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

        # Crea un DataFrame Panda 
        df_temp = pd.DataFrame([(tweet['name'], tweet['year'], tweet['rating'], flop_five_words)], 
                               columns=['name', 'year', 'rating', "flop_words"])

        if df_flop_review is None:
            df_flop_review = df_temp
        else:
            df_flop_review = pd.concat([df_flop_review, df_temp], ignore_index=True)

    return df_flop_review

df_flop_review = flop_tweet(df_tweet, spark)

if lista_select[18]:
    right_line9.header("$\\bold{Film}$ $\\bold{Flop}$ $\\bold{Tweet}$")
    right_line9.dataframe(df_flop_review, hide_index=True, column_config={
            "year": st.column_config.NumberColumn(format="%d")
        })

'''


