from linkedin_api import Linkedin
from credenciales import *
import json
import pandas as pd
from linkedin_api.client import Client
import pandas as pd
from pymongo import MongoClient

from linkedin_api.utils.helpers import (
    get_id_from_urn,
    get_urn_from_raw_update,
    get_list_posts_sorted_without_promoted,
    parse_list_raw_posts,
    parse_list_raw_urns,
    generate_trackingId,
    generate_trackingId_as_charString,
)
# Authenticate using any Linkedin account credentials
api = Linkedin(USER, PWD)



users_linkedin = api.search_people(keywords = 'Python')


# Conectar a MongoDB
#client = MongoClient("mongodb://localhost:27017/")
#db = client["linkedinapi"]
#collection = db["items-scala"]

df_people = pd.DataFrame(users_linkedin)
df_urn = df_people["urn_id"]
urn_values = list(df_urn)
list_limit = urn_values[:200]
rows = []

# Iterar sobre los resultados e imprimirlos
for urn in list_limit:

    resultados = api.get_profile_skills(urn_id = urn)

    # Extraer solo los valores del campo 'name'
    lista_items = [item['name'] for item in resultados]

    # Añadir un nuevo diccionario con los datos a la lista de filas
    rows.append({
        "urn": urn,
        "list_items": lista_items,
    })

# Convertir la lista de filas en un DataFrame
df = pd.DataFrame(rows)

# Conectar a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["linkedinapi"]
collection = db["items-python"]

# Insertar los datos en la colección
collection.insert_many(df.to_dict("records"))

