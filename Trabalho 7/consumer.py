from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pymysql

# Criar sessão Spark
spark = SparkSession.builder \
    .appName("KafkaReclamacoes") \
    .getOrCreate()

# Ler os dados do Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reclamacoes") \
    .load()

# Definir esquema dos dados
schema = "id INT, descricao STRING, data STRING"

# Transformar e selecionar dados do Kafka
value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Conectar ao banco de dados SQL para enriquecer os dados
def enrich_data(df):
    conn = pymysql.connect(host='localhost', user='usuario', password='senha', db='banco')
    cursor = conn.cursor()

    def enrich_row(row):
        cursor.execute(f"SELECT * FROM banco_dados WHERE id = {row['id']}")
        result = cursor.fetchone()
        enriched_data = (row['id'], row['descricao'], result[1])
        return enriched_data

    enriched_rdd = df.rdd.map(enrich_row)
    enriched_df = enriched_rdd.toDF(schema="id INT, descricao STRING, dados_banco STRING")

    cursor.close()
    conn.close()

    return enriched_df

# Enriquecer os dados
enriched_df = enrich_data(value_df)

# Salvar os dados enriquecidos em arquivos locais
query = enriched_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "./output") \
    .option("checkpointLocation", "./checkpoint") \
    .start()

query.awaitTermination()
