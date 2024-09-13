import pyspark
from pyspark.sql import SparkSession
from pymongo import MongoClient
import pandas as pd

# Crea una SparkSession
spark = SparkSession.builder \
    .appName("MongoSparkConnector3") \
    .getOrCreate()

# Conecta a MongoDB usando pymongo
client = MongoClient("mongodb://172.17.0.3:27017/")
db = client['docker']
collection = db['itemsscala']

# Extrae los datos desde MongoDB
mongo_data = list(collection.find())

# Convierte los datos a un DataFrame de pandas
pdf = pd.DataFrame(mongo_data)

    

#pdf['oc_item'] = pdf['list_items'].apply(conteo_items)
#print(pdf)

# Elimina la columna '_id' si es necesario, ya que no es serializable por defecto en Spark
if '_id' in pdf.columns:
    pdf = pdf.drop(columns=['_id'])

# Convierte el DataFrame de pandas a un DataFrame de Spark
df = spark.createDataFrame(pdf)

rdd = df.select("list_items").rdd
    
rdd_palabras = rdd.flatMap(lambda row: row.list_items)
rdd_mapsuma = rdd_palabras.map(lambda x: (x, 1)).reduceByKey(lambda x , y: x + y)

#rdd_sep = rdd_filtrado.map(lambda x: [[i] for i in x])
#rdd_sep_pablabras = rdd_sep.flatMap(lambda x: x.split('|'))

rdd_mapsumaSorted = rdd_mapsuma.map(lambda x: (x[1], x[0])).sortByKey()

# Recoger y mostrar los resultados
results = rdd_mapsumaSorted.collect()

print(results)
