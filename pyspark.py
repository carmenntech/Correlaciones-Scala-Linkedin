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
collection = db['items']

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
    
rdd_filtrado = rdd.flatMap(lambda row: row.list_items).collect()
#rdd_sep = rdd_filtrado.map(lambda x: [[i] for i in x])
#rdd_sep_pablabras = rdd_sep.flatMap(lambda x: x.split('|'))

for element in rdd_filtrado:
    print(element)



results = rdd_filtrado.collect()

print(results)