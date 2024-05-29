# -*- coding: utf-8 -*-
"""#3 - Homework - Homepage"""

## LIBRARIES IMPORT ##
from libraries import *

## PAGE CONFIGURATION ##
st.set_page_config(
    page_title='Movies',
    page_icon=':🎬:',
    layout='wide',
    initial_sidebar_state='expanded'  # Espandi la barra laterale inizialmente, se desiderato
    )
st.title('🎬📽️:rainbow[CINE-DATA: Dataset Cinematografici]🎞️🎥')


# CONNESSIONE AL CLUSTER
uri = "mongodb+srv://Homework3:Homework3@homework3.fc8huhi.mongodb.net/?retryWrites=true&w=majority&appName=Homework3"

# Create a new client and connect to the server
@st.cache_resource
def connect_to_Mongo(uri):
    client = MongoClient(uri)
    return client

client = connect_to_Mongo(uri)

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
collection_reviews = db['collection_reviews']  # Dataset delle recensioni

# Togliamo la colonna id dalla visualizzazione
collection_genre_list = list(collection_genre.aggregate([{"$project": {"_id": 0}}]))
collection_imdb_list = list(collection_imdb.aggregate([{"$project": {"_id": 0}}]))


## VISUALIZE DATASET ##
left_column, right_column = st.columns(2)
left_check = left_column.checkbox("IMDB Dataset")
right_check = right_column.checkbox("GENRE Dataset")

# Visualizza il dataset imdb
if left_check:
    left_column.dataframe(collection_imdb_list, hide_index=True, column_config = {
        "year": st.column_config.NumberColumn(format="%d"),
        "votes": st.column_config.NumberColumn(format="%d"),
        "rating": st.column_config.ProgressColumn(
            "Rating", width = "medium",
            min_value=0,
            max_value=10,  
            format="%.1f"
        )
    })

# Visualizza il dataset genre
if right_check:
    right_column.dataframe(collection_genre_list, hide_index=True, column_config = {
        "year": st.column_config.NumberColumn(format="%d"),
        "num_raters": st.column_config.NumberColumn(format="%d"),
        "num_reviews": st.column_config.NumberColumn(format="%d"),
        "review_url": st.column_config.LinkColumn(),
        "rating": st.column_config.ProgressColumn(
            "Rating", width = "medium",
            min_value=0,
            max_value=10,  
            format="%f",
        )
    })



### OPERATIONS ###
# Menu' di Checkbox
with st.sidebar:

    spazio = st.columns(3)
    spazio[1].header("$\color{red}\\bf{QUERY}$")
    st.subheader("",divider="rainbow")

    left_column_query, right_column_query = st.columns(2)

    # Lista delle query
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
def common_films(_collection_union):
    pipeline = [
        {
            "$facet": {
                "parte_1": [
                    {"$match": {"run_length": {"$ne": None}}},
                    {"$group": {"_id": {"title": "$title", "year": "$year"}}}
                ],
                "parte_2": [
                    {"$match": {"run_length": None}},
                    {"$group": {"_id": {"title": "$title", "year": "$year"}}}
                ]
            }
        },
        {
            "$project": {"risultato_completo": {"$concatArrays": ["$parte_1", "$parte_2"]}}
        },
        {
            "$unwind": "$risultato_completo"
        },
        {
            "$replaceRoot": {"newRoot": "$risultato_completo"}
        },
        {
            "$group": {
                "_id": {"title": "$_id.title", "year": "$_id.year"}, 
                "count": {"$sum": 1}
            }
        },
        {
            "$match": {"count": {"$gt": 1}}
        },
        {
            "$project": {
                "_id": 0, 
                "title": "$_id.title", 
                "year": "$_id.year"
            }
        }
    ]

    common_movies = list(collection_union.aggregate(pipeline))
    return common_movies
 
# Esegui l'operazione per ottenere i film comuni tra le due collection
common_movies = common_films(collection_union)
print(f"Common movies: {len(common_movies)}")

if lista_select[0]:
    left_query1.header("$\\bold{Common}$ $\\bold{Film}$")
    left_query1.dataframe(common_movies, hide_index=True, column_config={
        "year": st.column_config.NumberColumn(format="%d"),
        })


## TUTTI I FILM ##
@st.cache_resource
def all_films(_collection_union):
    pipeline = [
        {
            "$project": {
                "_id": 0, 
                "title": 1, 
                "year": 1
            }
        },
        {
            "$group": {"_id": {"title": "$title", "year": "$year"}}
        },
        {
            "$project": {
                "_id": 0, 
                "title": "$_id.title", 
                "year": "$_id.year"
            }
        }
    ]

    result = list(collection_union.aggregate(pipeline))
    return result
 
# Esegui la funzione per ottenere tutti i film
all_movies = all_films(collection_union)
 
# Converti la lista di documenti in un DataFrame Pandas
print(f"All movies: {len(all_movies)}")

if lista_select[1]:
    right_query1.header("$\\bold{All}$ $\\bold{Film}$")
    right_query1.dataframe(all_movies, hide_index=True, column_config={
        "year": st.column_config.NumberColumn(format="%d"),
        })


left_query2, right_query2 = st.columns(2)


## TOP 10 ##
@st.cache_resource
def best_film(_collection_union):
    pipeline = [
        {
            "$group": {
                "_id": {"title": "$title", "year": "$year"}, 
                "rating": {"$avg": "$rating"}
            }
        },
        {
            "$addFields": {"rating": {"$round": ["$rating", 1]}}
        },
        {
            "$sort": {"rating": -1}
        },
        {
            "$limit": 10
        },
        {
            "$project": {
                "_id": 0, 
                "title": "$_id.title", 
                "year": "$_id.year", 
                "rating": "$rating"
            }
        }
    ]

    risultato_finale = list(collection_union.aggregate(pipeline))
    return risultato_finale

top_10 = best_film(collection_union)

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


## FLOP 10 ##
@st.cache_resource
def flop_film(_collection_union):
    pipeline = [
        {
            "$group": {
                "_id": {"title": "$title", "year": "$year"}, 
                "rating": {"$avg": "$rating"}
            }
        },
        {
            "$addFields": {"rating": {"$round": ["$rating", 1]}}
        },
        {
            "$sort": {"rating": 1}
        },
        {
            "$limit": 10
        },
        {
            "$project": {
                "_id": 0, 
                "title": "$_id.title", 
                "year": "$_id.year", 
                "rating": "$rating"
                }
        }
    ]

    risultato_finale = list(collection_union.aggregate(pipeline))
    return risultato_finale

flop_10 = flop_film(collection_union)

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
def most_voted_film(_collection_union):
    pipeline = [
        {
            "$project": {
                "_id": 0,
                "title": 1,
                "year": 1,
                "votes": {"$max": ["$num_raters", "$votes"]}
            }
        },
        {
            "$group": {
                "_id": {"title": "$title", "year": "$year"},
                "votes": {"$max": "$votes"}
            }
        },
        {
            "$sort": {
                "votes": -1
            }
        },
        {
            "$limit": 1
        },
        {
            "$project": {
                "_id": 0,
                "title": "$_id.title",
                "year": "$_id.year",
                "votes": "$votes"
            }
        }
    ]
    
    risultato_finale = list(collection_union.aggregate(pipeline))
    return risultato_finale

most_voted = most_voted_film(collection_union)
   
if lista_select[4]:
    left_line3.header("$\\bold{Most}$ $\\bold{Voted}$")
    left_line3.dataframe(most_voted, hide_index=True, column_config={
        "year": st.column_config.NumberColumn(format="%d"),
        "votes": st.column_config.NumberColumn(format="%d")
    })


## FILM RECENSITO DA PIU' UTENTI ##
@st.cache_resource
def most_reviewed_film(_collection_union):
    pipeline = [
        {
            "$match": {"num_reviews": {"$ne": None}}
        },
        {
            "$group": {
                "_id": {"title": "$title", "year": "$year"},
                "num_reviews": {"$avg": "$num_reviews"}
            }
        },
        {
            "$sort": {
                "num_reviews": -1
            }
        },
        {
            "$limit": 1
        },
        {
            "$project": {
                "_id": 0, 
                "title": "$_id.title", 
                "year": "$_id.year", 
                "num_reviews": "$num_reviews"
                }
        }
    ]
    
    risultato_finale = list(collection_union.aggregate(pipeline))
    return risultato_finale

most_reviewed = most_reviewed_film(collection_union)

if lista_select[5]:
    right_line3.header("$\\bold{Most}$ $\\bold{Reviewed}$")
    right_line3.dataframe(most_reviewed, hide_index=True, column_config={
        "year": st.column_config.NumberColumn(format="%d"),
        "num_reviews": st.column_config.NumberColumn("reviews", format="%d")
    })


left_line4, right_line4 = st.columns(2)

## FILM PER GENERE ##
@st.cache_resource
def film_x_genre(_collection_union):
    pipeline = [
        {
            "$project": {
                "title": 1, 
                "year": 1, 
                "genre": {"$split": ["$genre", ","]}, 
                "rating": 1
                }
        },
        {
            "$unwind": "$genre"
        },
        {
            "$group": {"_id": {"title": "$title", "year": "$year", "genre": "$genre"}}
        },
        {
            "$group": {
                "_id": "$_id.genre", 
                "count": {"$count": {}}
                }
        },
        {
            "$project": {
                "_id": 0, 
                "genre": "$_id", 
                "count": "$count"
                }
        }
    ]

    
    risultato_finale = list(collection_union.aggregate(pipeline))
    return risultato_finale

film_x_genre = film_x_genre(collection_union)

if lista_select[6]:
    left_line4.header("$\\bold{Film}$ $\\bold{x}$ $\\bold{Genre}$")
    left_line4.dataframe(film_x_genre, hide_index=True, column_config={
        "count": st.column_config.NumberColumn(format="%d")
        })



## MEDIA VOTI PER GENERE ##
@st.cache_resource
def mean_genre(_collection_union):
    pipeline = [
        {
            "$project": {
                "title": 1, 
                "year": 1, 
                "genre": {"$split": ["$genre", ","]}, 
                "rating": 1
                }
        },
        {
            "$unwind": "$genre"
        },
        {
            "$group": {
                "_id": {"title": "$title", "year": "$year", "genre": "$genre"}, 
                "rating": {"$avg": "$rating"}
                }
        },
        {
            "$group": {
                "_id": "$_id.genre", 
                "rating": {"$avg": "$rating"}
                }
        },
        {
            "$addFields": {"rating": {"$round": ["$rating", 1]}}
        },
        {
            "$project": {
                "_id": 0,
                "genre": "$_id",
                "rating": "$rating"
                }
        }
    ]

    risultato_finale = list(collection_union.aggregate(pipeline))
    return risultato_finale

df_mean_genre_with_count = mean_genre(collection_union)

if lista_select[7]:
    right_line4.header("$\\bold{Votes}$ $\\bold{x}$ $\\bold{Genre}$")
    right_line4.dataframe(df_mean_genre_with_count, hide_index=True, column_config={
        "rating": st.column_config.ProgressColumn(
            min_value=0,
            max_value=10,  
            format="%f",
        )
    })


left_line5, right_line5 = st.columns(2)


## DURATA MEDIA DI UN FILM ##
@st.cache_resource
def mean_time(_collection_union):
    pipeline = [
        {
            "$match": {"run_length": {"$ne": None}}
        },
        {
            "$group": {"_id": None, "run_length": {"$avg": "$run_length"}}
        },
        {
            "$project": { "_id": 0, "run_length": {"$round": ["$run_length", 1]}}
        }
    ]

    risultato_finale = list(collection_union.aggregate(pipeline))
    return risultato_finale

df_time = mean_time(collection_union)

if lista_select[8]:
    left_line5.header("$\\bold{Mean}$ $\\bold{Time}$ \
                      $\\bold{x}$ $\\bold{Film}$")
    left_line5.dataframe(df_time, hide_index=True)

    
## DURATA MEDIA PER GENERE ##
@st.cache_resource
def mean_time_genre(_collection_union):
    pipeline = [
        {
            "$match": {"run_length": {"$ne": None}}
        },
        {
            "$project": {
                "title": 1, 
                "year": 1, 
                "genre": {"$split": ["$genre", ","]}, 
                "run_length": 1
            }
        },
        {
            "$unwind": "$genre"
        },
        {
            "$group": {
                "_id": {"title": "$title", "year": "$year", "genre": "$genre"}, 
                "run_length": {"$avg": "$run_length"}
            }
        },
        {
            "$group": {
                "_id": "$_id.genre", 
                "run_length": {"$avg": "$run_length"}
                }
        },
        {
            "$addFields": {"run_length": {"$round": ["$run_length", 1]}}
        },
        {
            "$sort": {"run_length": -1}
        },
        {
            "$project": {
                "_id": 0, 
                "genre": "$_id", 
                "run_length": "$run_length"
            }
        }
    ]

    risultato_finale = list(collection_union.aggregate(pipeline))
    return risultato_finale

df_time_genre = mean_time_genre(collection_union)

if lista_select[9]:
    right_line5.header("$\\bold{Mean}$ $\\bold{Time}$ \
                        $\\bold{x}$ $\\bold{Genre}$")
    right_line5.dataframe(df_time_genre, hide_index=True)


## RATING PONDERATO ##
@st.cache_resource
def rating_ponderato(_collection_union):
    pipeline = [
        {
            "$project": {
                "title": 1, 
                "year": 1, 
                "rating": 1, 
                "votes": {"$max": ["$num_raters", "$votes"]}
            }
        },
        {
            "$group": {
                "_id": {"title": "$title", "year": "$year"}, 
                "rating": {"$avg": "$rating"},
                "votes": {"$max": "$votes"}
            }
        },
        # Calcola il rating ponderato
        {
            "$addFields": {
                "weighted_rating": {
                    "$round": [{"$multiply": ["$rating", {"$log10": "$votes"}]}, 1]
                }
            }
        },
        {
            "$sort": {
                "weighted_rating": -1, 
                "rating": -1
            }
        },
        {
            "$limit": 10
        },
        {
            "$project": {
                "_id": 0,
                "title": "$_id.title",
                "year": "$_id.year",
                "weighted_rating": "$weighted_rating",
                "rating": "$rating",
                "votes": "$votes"
            }
        }
    ]

    risultato_finale = list(collection_union.aggregate(pipeline))
    return risultato_finale

best_movies = rating_ponderato(collection_union)

if lista_select[10]:
    st.header("$\\bold{Weighted}$ $\\bold{Rating}$")
    st.dataframe(best_movies, use_container_width=True, hide_index=True, column_config={
        "rating": st.column_config.ProgressColumn(
            min_value=0,
            max_value=10,  
            format="%f",
        ),
        "votes": st.column_config.NumberColumn(format="%d"),
        "year": st.column_config.NumberColumn(format="%d")
    })
    
    st.write("""
            ### Nota:
            Il rating ponderato è una metrica che tiene conto sia del rating di un film che del numero di voti ricevuti.
            La formula usata è la seguente:
        
            `weighted_rating = rating * log10(votes)`
        
            Dove:
            - `rating` è il rating del film
            - `votes` è il numero di voti ricevuti
            - `log10` è il logaritmo in base 10
            """)    


## TOP FILM PER CADENZA DECENNALE ##
@st.cache_resource
def top_decennio(_collection_union):
    pipeline = [
        {
            "$group": {
                "_id": {"title": "$title", "year": "$year"}, 
                "rating": {"$avg": "$rating"}
            }
        },
        {
            "$addFields": {"rating": {"$round": ["$rating", 1]}}
        },
        {   
            "$project": {
                "_id": 0,
                "title": "$_id.title",
                "year": "$_id.year",
                "rating": "$rating"
            }
        },
        {
            "$addFields": {"decade": {"$multiply": [{"$floor": {"$divide": ["$year", 10]}}, 10]}}
        },
        {
            "$group": {
                "_id": "$decade",
                "films": {"$push": {"title": "$title", "year": "$year", "rating": "$rating", "decade": "$decade"}}
            }
        },
        {
            "$addFields": {"films": {"$slice": [{"$sortArray": {"input": "$films", "sortBy": { "rating": -1}}}, 5]}}
        },
        {
            "$unwind": "$films"
        },
        {
            "$replaceRoot": {"newRoot": "$films"}
        },
        {
            "$sort": {
                "decade": 1,
                "rating": -1
            }
        },
        {
            "$project":{
                "title": "$title",
                "year": "$year",
                "rating": "$rating"
            }
        }
    ]

    risultato_finale = list(collection_union.aggregate(pipeline))
    return risultato_finale

df_top_decade = top_decennio(collection_union)

if lista_select[11]:
    st.header("$\\bold{Film}$ $\\bold{x}$ $\\bold{Decade}$")
    st.dataframe(df_top_decade, use_container_width=True, hide_index=True, column_config={
            "year": st.column_config.NumberColumn(format="%d")
        })
    

## QUALE DECENNIO HA DATO I FILM MIGLIORI MEDIAMENTE E IN ASSOLUTO ##
@st.cache_resource
def top_10_decennio(_collection_union):
    pipeline_max = [
        {
            "$group": {
                "_id": {"title": "$title", "year": "$year"}, 
                "rating": {"$avg": "$rating"}
            }
        },
        {
            "$addFields": {"rating": {"$round": ["$rating", 1]}}
        },
        {   
            "$project": {
                "_id": 0,
                "title": "$_id.title",
                "year": "$_id.year",
                "rating": "$rating"
            }
        },
        {
            "$addFields": {"decade": {"$multiply": [{"$floor": {"$divide": ["$year", 10]}}, 10]}}
        },
        {
            "$group": {
                "_id": "$decade",
                "rating": {"$max": "$rating"}
            }
        },
        {
            "$sort": {
                "_id": 1
            }
        },
        {
            "$project":{
                "_id": 0,
                "decade": "$_id",
                "rating": "$rating"
            }
        }
    ]

    pipeline_mean = [
        {
            "$group": {
                "_id": {"title": "$title", "year": "$year"}, 
                "rating": {"$avg": "$rating"}
            }
        },
        {
            "$addFields": {"rating": {"$round": ["$rating", 1]}}
        },
        {   
            "$project": {
                "_id": 0,
                "title": "$_id.title",
                "year": "$_id.year",
                "rating": "$rating"
            }
        },
        {
            "$addFields": {"decade": {"$multiply": [{"$floor": {"$divide": ["$year", 10]}}, 10]}}
        },
        {
            "$group": {
                "_id": "$decade",
                "rating": {"$avg": "$rating"}
            }
        },
        {
            "$sort": {
                "_id": 1
            }
        },
        {
            "$project":{
                "_id": 0,
                "decade": "$_id",
                "rating": {"$round": ["$rating", 1]}
            }
        }
    ]

    risultato_finale_max = list(collection_union.aggregate(pipeline_max))
    risultato_finale_mean = list(collection_union.aggregate(pipeline_mean))

    return risultato_finale_max, risultato_finale_mean 

df_10_max, df_10_mean = top_10_decennio(collection_union)

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
def actors(_collection_union):
    pipeline = [
        {
            "$match": {"stars": {"$ne": None}}
        },
        {
            "$project": {"stars": {"$split": ["$stars", ","]}}
        },
        {
            "$unwind": "$stars"
        },
        {
            "$group": {"_id": "$stars", "num_movies": {"$count": {}}}
        },
        {
            "$sort": {"num_movies": -1}
        },
        {
            "$limit": 10
        },
        {
            "$project": {
                "_id": 0,
                "stars": "$_id",
                "num_movies": "$num_movies"}
        }
    ]

    risultato_finale = list(collection_union.aggregate(pipeline))
    return risultato_finale

df_stars = actors(collection_union)

if lista_select[14]:
    left_line8.header("$\\bold{Stars}$")
    left_line8.dataframe(df_stars, hide_index=True, column_config={
        "movies_number": st.column_config.NumberColumn(format="%d ⭐")
    })


## ATTORI PIU' PRESENTI CHE HANNO FATTO I FILM PIU' APPREZZATI ##
@st.cache_resource
def star_x_film(_collection_union):
    pipeline = [
        {
            "$match": {"stars": {"$ne": None}}
        },
        {
            "$project": {
                "title": "$title", 
                "year": "$year", 
                "stars": {"$split": ["$stars", ","]}, 
                "rating": "$rating"
            }
        },
        {
            "$facet": {
                # Conteggio dei film per ogni attore
                "parte_1": [
                    {"$unwind": "$stars"},
                    {"$group": {"_id": "$stars", "num_movies": {"$sum": 1}}}
                ],
                # Prende la top 10 dei film
                "parte_2": [
                    {"$sort": {"rating": -1}},
                    {"$limit": 10},
                    {"$unwind": "$stars"}
                ]
            }
        },
        {
            "$project": {
                "parte_1": 1,
                "parte_2": 1
            }
        },
        {
            "$unwind": "$parte_1"
        },
        {
            "$unwind": "$parte_2"
        },
        {
            "$match": {"$expr": {"$eq": ["$parte_1._id", "$parte_2.stars"]}}
        },
        {
            "$group": {
                "_id": "$parte_2.stars",
                "title": {"$first": "$parte_2.title"},
                "rating": {"$first": "$parte_2.rating"},
                "num_movies": {"$first": "$parte_1.num_movies"}
            }
        },
        {
            "$sort": {"num_movies": -1}
        },
        {
            "$limit": 10
        },
        {
            "$project": {
                "_id": 0,
                "title": "$title",
                "stars": "$_id",
                "rating": "$rating",
                "num_movies": "$num_movies"
            }
        }
    ]

    risultato_finale = list(collection_union.aggregate(pipeline))
    return risultato_finale

df_stars_film = star_x_film(collection_union)

if lista_select[15]:
    right_line8.header("$\\bold{Stars}$ $\\bold{Most}$ $\\bold{Film}$")
    right_line8.dataframe(df_stars_film, hide_index=True)


## ATTORI E REGISTI CON PIU' COLLABORAZIONI ##
@st.cache_resource
def actor_director(_collection_union):
    pipeline = [
        {
            "$match": {"stars": {"$ne": None}, "director": {"$ne": None}}
        },
        {
            "$project": {
                "actor": {"$split": ["$stars", ","]},
                "director": {"$arrayElemAt": [{"$split": ["$director", ","]}, 0]}
            }
        },
        {
            "$unwind": "$actor"
        },
        {
            "$match": {"$expr": {"$ne": ["$actor", "$director"]}}
        },
        {
            "$group": {
                "_id": {"director": "$director", "actor": "$actor"},
                "movies_number": {"$sum": 1}
            }
        },
        {
            "$sort": {"movies_number": -1}
        },
        {
            "$limit": 10
        },
        {
            "$project": {
                "_id": 0,
                "director": "$_id.director",
                "actor": "$_id.actor",
                "movies_number": "$movies_number"
            }
        }
    ]
    
    risultato_finale = list(collection_union.aggregate(pipeline))
    return risultato_finale

df_couple = actor_director(collection_union)

if lista_select[16]:
    st.header("$\\bold{Actor}$ $\\bold{x}$ $\\bold{Director}$")
    st.dataframe(df_couple, hide_index=True)


left_line9, right_line9 = st.columns(2)


## COMMENTI TOP 10 ##
@st.cache_resource
def top_tweet(_collection_reviews):
    pipeline = [
        # Step 1: Filtrare i top 10 film e aggiungere le recensioni
        {
            "$sort": {"rating": -1}
        },
        {
            "$limit": 10
        },
        # Step 2: Esplodere le recensioni in singole parole
        {
            "$project": {
                "title": 1,
                "year": 1,
                "rating": 1,
                "words": {"$split": ["$review_text", " "]}
            }
        },
        {
            "$unwind": "$words"
        },
        # Step 3: Rimuovere le stop words
        {
            "$match": {
                "words": {
                    "$nin": ['and', 'to', 'the', 'into', 'on', 'of', 'by', 'or', 'in', 'a',
                            'with', 'that', 'she', 'it', 'i', 'you', 'he', 'we', 'they',
                            'her', 'his', 'its', 'this', 'that', 'at', 'as', 'for', 'not',
                            'so', 'do', 'is', 'was', 'are', 'have', 'has', 'an', 'my', '-',
                            'but', 'be', 'film', 'movie', 'one', 'from', 'it\'s', 'me',
                            'where', '']
                }
            }
        },
        # Step 4: Contare le parole
        {
            "$group": {
                "_id": {"title": "$title", "year": "$year", "rating": "$rating", "word": "$words"},
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        },
        # Step 5: Raggruppare per film e ottenere le top 5 parole
        {
            "$group": {
                "_id": {"title": "$_id.title", "year": "$_id.year", "rating": "$_id.rating"},
                "words": {"$push": {"word": "$_id.word", "count": "$count"}}
            }
        },
        {
            "$project": {
                "_id": 0,
                "title": "$_id.title",
                "year": "$_id.year",
                "rating": "$_id.rating",
                "top_word_count": {
                    "$slice": [
                        {
                            "$map": {
                                "input": {"$slice": ["$words", 5]},
                                "as": "word",
                                "in": {"$concat": ["$$word.word", " = ", {"$toString": "$$word.count"}]}
                            }
                        },
                        5
                    ]
                }
            }
        },
        {
            "$sort": {"rating": -1}  
        }
    ]

    risultato_finale = list(collection_reviews.aggregate(pipeline))
    return risultato_finale

df_top_review = top_tweet(collection_reviews)

if lista_select[17]:
    left_line9.header("$\\bold{Film}$ $\\bold{Top}$ $\\bold{Tweet}$")
    left_line9.dataframe(df_top_review, hide_index=True, column_config={
            "year": st.column_config.NumberColumn(format="%d")
        })


## COMMENTI FLOP 10 ## 
@st.cache_resource
def flop_tweet(_collection_reviews):
    pipeline = [
        # Step 1: Filtrare i flop 10 film e aggiungere le recensioni
        {
            "$sort": {"rating": 1}
        },
        {
            "$limit": 11
        },
        # Step 2: Esplodere le recensioni in singole parole
        {
            "$project": {
                "title": 1,
                "year": 1,
                "rating": 1,
                "words": {"$split": ["$review_text", " "]}
            }
        },
        {
            "$unwind": "$words"
        },
        # Step 3: Rimuovere le stop words
        {
            "$match": {
                "words": {
                    "$nin": ['and', 'to', 'the', 'into', 'on', 'of', 'by', 'or', 'in', 'a',
                            'with', 'that', 'she', 'it', 'i', 'you', 'he', 'we', 'they',
                            'her', 'his', 'its', 'this', 'that', 'at', 'as', 'for', 'not',
                            'so', 'do', 'is', 'was', 'are', 'have', 'has', 'an', 'my', '-',
                            'but', 'be', 'film', 'movie', 'one', 'from', 'it\'s', 'me',
                            'where', '']
                }
            }
        },
        # Step 4: Contare le parole
        {
            "$group": {
                "_id": {"title": "$title", "year": "$year", "rating": "$rating", "word": "$words"},
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        },
        # Step 5: Raggruppare per film e ottenere le top 5 parole
        {
            "$group": {
                "_id": {"title": "$_id.title", "year": "$_id.year", "rating": "$_id.rating"},
                "words": {"$push": {"word": "$_id.word", "count": "$count"}}
            }
        },
        {
            "$project": {
                "_id": 0,
                "title": "$_id.title",
                "year": "$_id.year",
                "rating": "$_id.rating",
                "flop_word_count": {
                    "$slice": [
                        {
                            "$map": {
                                "input": {"$slice": ["$words", 5]},
                                "as": "word",
                                "in": {"$concat": ["$$word.word", " = ", {"$toString": "$$word.count"}]}
                            }
                        },
                        5
                    ]
                }
            }
        },
        {
            "$sort": {"rating": 1}  
        }
    ]

    risultato_finale = list(collection_reviews.aggregate(pipeline))
    return risultato_finale
    

df_flop_review = flop_tweet(collection_reviews)

if lista_select[18]:
    right_line9.header("$\\bold{Film}$ $\\bold{Flop}$ $\\bold{Tweet}$")
    right_line9.dataframe(df_flop_review, hide_index=True, column_config={
            "year": st.column_config.NumberColumn(format="%d")
        })
    
    