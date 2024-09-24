from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Defina o esquema dos dados
schema = StructType().add("id", StringType()).add("data", StringType())

spark = SparkSession.builder \
    .appName("Kafka-PySpark-Structured-Streaming") \
    .getOrCreate()

# Ler dados do Kafka
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "data_topic") \
    .load()

# Convertendo os dados para o esquema desejado
data_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Conectar ao banco de dados SQL para enriquecimento
def enrich_data(df):
    enriched_df = df.withColumn("enriched_field", df["data"] + "_enriched")
    return enriched_df

enriched_stream = data_stream.transform(enrich_data)

# Salvando os dados em um arquivo local
query = enriched_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/path/to/save/enriched_data/") \
    .option("checkpointLocation", "/path/to/checkpoints/") \
    .start()

query.awaitTermination()
