from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, split
from pyspark.sql.types import StructType, StringType
import mysql.connector

# Criar a sessão Spark
spark = SparkSession.builder \
    .appName("Kafka-PySpark-CSV-To-MySQL") \
    .getOrCreate()

# Esquema genérico para os dados CSV lidos do Kafka
schema = StructType().add("id", StringType()).add("name", StringType()).add("value", StringType())

# Ler os dados do Kafka
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "csv_data_topic") \
    .load()

# Transformar a string CSV de volta em colunas
data_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", split(col("value"), ",")) \
    .select(
        col("data")[0].alias("id"),
        col("data")[1].alias("name"),
        col("data")[2].alias("value")
    )

# Função para enriquecer os dados consultando o MySQL
def enrich_data(df):
    # Conectar ao MySQL
    connection = mysql.connector.connect(
        host='localhost',
        database='mydb',
        user='myuser',
        password='mypassword'
    )

    cursor = connection.cursor(dictionary=True)

    # Suponha que a tabela de enriquecimento tem um mapeamento de id -> enriched_value
    query = "SELECT id, enriched_value FROM enrichment_table WHERE id = %s"

    def enrich_row(row):
        cursor.execute(query, (row['id'],))
        result = cursor.fetchone()
        if result:
            return row['id'], row['name'], row['value'], result['enriched_value']
        return row['id'], row['name'], row['value'], None

    # Aplicar enriquecimento em cada linha
    enriched_data = df.rdd.map(lambda row: enrich_row(row.asDict()))

    # Criar um novo DataFrame com os dados enriquecidos
    enriched_df = spark.createDataFrame(enriched_data, schema=['id', 'name', 'value', 'enriched_value'])

    connection.close()
    return enriched_df

# Aplicar enriquecimento
enriched_stream = enrich_data(data_stream)

# Escrever os dados enriquecidos em formato Parquet
query = enriched_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/path/to/save/enriched_data/") \
    .option("checkpointLocation", "/path/to/checkpoints/") \
    .start()

# Manter o stream rodando
query.awaitTermination()
