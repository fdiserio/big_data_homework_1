# -*- coding: utf-8 -*-
"""#3 - Homework - LoadData"""

## LIBRARIES IMPORT ##
from libraries import *

## CONNESSIONE AL CLUSTER ##
uri = "mongodb+srv://Homework3:Homework3@homework3.fc8huhi.mongodb.net/?retryWrites=true&w=majority&appName=Homework3"

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
collection_reviews = db['collection_reviews']  # Dataset delle recensioni


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


### VERIFICA ESISTENZA DATABASE ###
# Se il database esiste già, lo elimina per evitare inserimenti ripetuti
client.drop_database(db)

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


### CREAZIONE COLLECTION REVIEWS ###
top_10_films = list(collection_union.find({"review_url": {"$ne": None}}).sort("rating", -1).limit(10))
flop_10_films = list(collection_union.find({"review_url": {"$ne": None}}).sort("rating", 1).limit(11))

# Funzione per recuperare la recensione da un URL
def get_review_from_url(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            comments = soup.find_all('div', class_='text show-more__control')
            reviews = [comment.text.strip() for comment in comments]
            text = ' '.join(reviews)
            text = text.replace(",", "").replace(";", "").replace(":", "")\
                       .replace("(", "").replace(")", "").replace("!", "")\
                       .replace("?", "").replace(".", "")
            text = text.lower()
            return text
        else:
            return "Failed to fetch HTML from URL"
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        return "Exception occurred"

for film in top_10_films:
    review_text = get_review_from_url(film['review_url'])
    film_data = {
        "title": film["title"],
        "year": film["year"],
        "rating": film["rating"],
        "review_text": review_text
    }
    collection_reviews.insert_one(film_data)
    
for film in flop_10_films:
    review_text = get_review_from_url(film['review_url'])
    film_data = {
        "title": film["title"],
        "year": film["year"],
        "rating": film["rating"],
        "review_text": review_text
    }
    collection_reviews.insert_one(film_data)

# Stampa stringa di fine creazione e conteggio
print("Creata collection delle recensioni")
count_reviews = list(collection_reviews.aggregate([{'$count': 'count'}]))[0]['count']
print(f"Ci sono {count_reviews} film nella collection reviews")


### CHIUSUSRA CONNESSIONE CLUSTER ###
client.close()