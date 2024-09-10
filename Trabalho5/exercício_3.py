from pyspark.sql import SparkSession

def read_files(spark, source_path, delimiter= None, format="csv"):

  print(f"Leia os arquivos file/folder: {source_path} usando delimitador {delimiter}")
  df = spark.read.format(format).options(delimiter=delimiter, header=True, inferSchema=True, ).load(source_path)

  print("Total rows:",df.count())

  return df

def write_parquet_files(spark,df,destination_folder):
  df.write.mode('overwrite').parquet(destination_folder)

def decode_names(spark, df, folder_path):
  from pyspark.sql import SparkSession
  from pyspark.sql.functions import udf, col, lit, broadcast
  from pyspark.sql.types import StringType


  correct_dic = {
      "CR�DITO": "CRÉDITO",
      "MUNIC�PIO": "MUNICÍPIO",
      "UB�": "UBÁ",
      "S�O": "SÃO",
      "M�TUO": "MÚTUO",
      "CONFEDERA��O": "CONFEDERAÇÃO",
      "REGI�O": "REGIÃO",
      "AMAZ�NIA": "AMAZÔNIA",
      "ADMISS�O": " ADMISSÃO",
      "TRANSAMAZ�NICA": "TRANSAMAZÔNICA",
      "GOI�S": "GOIÁS",
      "INTERA��O": "INTERAÇÃO",
      "SOLID�RIA": "SOLIDÁRIA",
      "AG�NCIA": "AGÊNCIA",
      "RIBEIR�O": "RIBEIRÃO",
      "TOP�ZIO": "TOPÁZIO",
      "PIAU�": "PIAUÍ",
      'PARAN�': 'PARANÁ',
      'ALIAN�A': 'ALIANÇA',
      'JACU�': 'JACUÍ',
      'PRODU��O': 'PRODUÇÃO',
      'PAR�': 'PARÁ',
      'ITAJA�': 'ITAJAÍ',
      'INTEGRA��O': 'INTEGRAÇÃO',
      'GA�CHO': 'GAÚCHO',
      'UNI�O': 'UNIÃO',
      'CREDIGUA�U': 'CREDIGUAÇU',
      'EMPRES�RIOS': 'EMPRESÁRIOS',
      'IGUA�U': 'IGUAÇU',
      'PARAN�/SÃO': 'PARANÁ/SÃO',
      'PARA�BA': 'PARAÍBA',
      'M�DICOS': 'MÉDICOS',
      'SA�DE': 'SAÚDE',
      'SOLID�RIO': 'SOLIDÁRIO',
      'POUPAN�A': 'POUPANÇA',
      'ELETROBR�S': 'ELETROBRÁS',
      'NEG�CIOS': 'NEGÓCIOS',
      'SEGURAN�A': 'SEGURANÇA',
      'P�BLICA': 'PÚBLICA',
      'T�TULO': 'TÍTULO',
      'FAM�LIA': 'FAMÍLIA',
      'M�XIMA': 'MÁXIMA',
      'MONOP�LIO': 'MONOPÓLIO',
      'C�MBIO': 'CÂMBIO',
      '�REA': 'ÁREA',
      'TEND�NCIA': 'TENDÊNCIA',
      'M�DICOS,': 'MÉDICOS',
      'CI�NCIAS': 'CIÊNCIAS',
      'FARMAC�UTICA': 'FARMACÊUTICA',
      'ESP�RITO': 'ESPÍRITO',
      'PARTICIPA��ES': 'PARTICIPAÇÕES',
      'SERVI�OS': 'SERVIÇO',
      'EMPR�STIMO': 'EMPRÉSTIMO',
      'FUNCION�RIOS': 'FUNCIONÁRIOS',
      'GOI�NIA': 'GOIÂNIA',
      'ROND�NIA': 'RONDÔNIA',
      'DIVIN�POLIS': 'DIVINÓPOLIS',
      'ITA�NA': 'ITAÚNA',
      'SEBASTI�O': 'SEBASTIÃO',
  }
  # Broadcast the dictionary
  broadcast_dict = spark.sparkContext.broadcast(correct_dic)


  replace_udf = udf(lambda text: ''.join(broadcast_dict.value.get(char, char) for char in text), StringType())

  print("Linhas com caracteres incorretos:")
  df.filter(col('Nome').contains('�')).show(truncate=False)


  # Create a UDF for the replacement function
  df = df.withColumn("Nome", replace_udf(col("Nome")))
  print("Após correção:")
  df.show(truncate=False)

  df_uncorrected_rows = df.filter(col('Nome').contains('�'))
  print("Linhas com caracteres incorretos:")
  df_uncorrected_rows.show(truncate=False)

  if df_uncorrected_rows.rdd.isEmpty():
     print("Nenhuma linha contendo o caracter '�' foi encontrada")
  else:
    destination_path = os.path.join("/content/sample_data/trusted/erros", "incorrected_rows.csv")
    df_uncorrected_rows.write.csv(destination_path, header=True, mode="overwrite")
    print(f"Linhas contendo o caracter '�' foram salvas em {destination_path}")

  return df

def transformation_trusted(spark, folder_name, folder_path, df):
  from pyspark.sql.functions import regexp_replace, col, row_number, when, isnull
  from pyspark.sql.window import Window

  print(folder_name)
  print("Quantidade total de linhas antes da limpeza dos dados:",df.count())
  if folder_name == "Bancos":
    # A solução segue o mesmo principio do windows function row_number() no SQL
    #Particiona a base por CNPJ e ordena pelo nome, e pega apenas o primeiro registro
    window = Window.partitionBy("CNPJ").orderBy(row_number().over(Window.partitionBy("CNPJ").orderBy("Nome")))
    df = df.withColumn("row_num", row_number().over(window))
    df = df.filter(col("row_num") == 1)
    df = df.drop("row_num")

    # Remove "- PRUDENTIAL" dos nomes
    df = df.withColumn("Nome Original", col("Nome"))
    df = df.withColumn("Nome", regexp_replace(col("Nome"), "- PRUDENCIAL", ""))

    print("Limpeza realizada com sucesso: dados deduplicados e remoção do sufixo '- PRUDENCIAL' da coluna Nome")

    df = decode_names(spark, df, folder_path)
  elif folder_name == "Empregados":
      df = df.withColumnRenamed("CNPJ", "CNPJ_Segmento")
      df = df.withColumnRenamed("Segmento", "CNPJ_Segmento")
  elif folder_name == "Reclamações":
      df = df.withColumnRenamed("CNPJ IF", "CNPJ")
  df.show()
  print("Quantidade total de linhas após da limpeza dos dados:",df.count())
  return df

def transformation_delivery(df_bancos,df_empregados, df_reclamacoes):
  df_empregados_renamed = df_empregados.withColumnRenamed("CNPJ_Segmento", "CNPJ")
  df_empregados_renamed = df_empregados_renamed.drop("Nome")
  # df_reclamacoes_renamed = df_reclamacoes.withColumnRenamed("CNPJ IF", "CNPJ")

  df_join = df_bancos.join(df_empregados_renamed, "CNPJ", "inner").join(df_reclamacoes, "CNPJ", "inner")
  print("Junção das tabelas:")
  df_join.show(truncate=False)
  print("Total de linhas: ",df_join.count())
  return df_join

def load_dataframe_to_postgres(df, table_name, schema_name):
  # PostgreSQL connection details


  host = "localhost"
  database = "trabalho_3"
  user = "postgres"
  password = "growdev"

  # Construct JDBC URL with schema
  jdbc_url = f"jdbc:postgresql://{host}/{database}?currentSchema={schema_name}"

  # Write DataFrame to PostgreSQL
  df.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "your_table") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()

"""## 1. Lemos o arquivo csv e tsv, salvamos no format parquet em camadas (raw, trusted e delivery), e depois fizemos load para postgre para cada camada

# Ingestão na camada raw + trusted
"""

import os

spark = SparkSession.builder \
    .appName("Exercício 3") \
    .getOrCreate()

list_folders = [
  {"folder_name":"Bancos", "delimiter": "\t"},
  {"folder_name":"Reclamações", "delimiter": ";"},
  {"folder_name":"Empregados", "delimiter": "|"},
]

root = '/content/sample_data'
for folder in list_folders:
    print("\nProcessing folder folder...")
    folder_name = folder["folder_name"]
    delimiter = folder["delimiter"]
    table_name = folder["folder_name"].lower()
    folder_path = os.path.join(root,folder_name)
    file_list = os.listdir(folder_path)

    #Verifica qual é o tipo do arquivo, e só aceita se for csv ou tsv
    #Defini o schema com base no primeiro arquivo
    if file_list[0].endswith('.csv') or file_list[0].endswith('.tsv'):
      df_raw = read_files(spark,os.path.join(folder_path,file_list[0]),delimiter, "csv")

      # if folder_name == "Empregados":
      #   df_raw = df_raw.withColumnRenamed("CNPJ", "CNPJ_Segmento")
    for file_name in file_list[1:]:
        source_file_path = os.path.join(folder_path,file_name)
        if file_name.endswith('.csv') or file_name.endswith('.tsv'):
          temp_df = read_files(spark,source_file_path,delimiter, "csv")
          # if folder_name == "Empregados":
          #   temp_df = temp_df.withColumnRenamed("CNPJ", "CNPJ_Segmento")
          df_raw = df_raw.union(temp_df)
          print(f"Total linhas {table_name}:",df_raw.count())

    df_raw.show(truncate=False)
    print(f"Total linhas {table_name}:",df_raw.count())


    ### Camada raw ###
    raw_folder = os.path.join(root, "raw", table_name)
    print(f"\n Escrevendo arquivos de {folder_name} em parquet...")
    print(f"\n Dataframe resultado da camada raw:")
    df_raw.show(truncate=False)
    write_parquet_files(spark, df_raw, raw_folder)
    # load_dataframe_to_postgres(df_raw, table_name, "raw")


    ### Camada trusted ###

    print(f"Limpando os dados da tabela {folder_name}...")
    trusted_folder = os.path.join(root, "trusted", table_name)
    df_trusted = transformation_trusted(spark, folder_name, trusted_folder, df_raw)
    # Escrever na camada trusted
    print("\n Escrevendo em arquivo parquet...")
    write_parquet_files(spark, df_trusted, trusted_folder)
    print("\n Dataframe resultado da camada trusted:")
    df_trusted.show(truncate=False)
    # load_dataframe_to_postgres(df_trusted, table_name, "trusted")

"""## Ingestão na camada delivery"""

### Camada Delivery ###

#Ler dados da camada trusted
print("\n Lendo arquivos parquet da camada trusted")

df_bancos = read_files(spark,os.path.join(root, "trusted", "bancos"),delimiter, "parquet")
df_empregados = read_files(spark,os.path.join(root, "trusted", "empregados"),delimiter, "parquet")
df_reclamacoes = read_files(spark,os.path.join(root, "trusted", "reclamações"),delimiter, "parquet")


df_delivery = transformation_delivery(df_bancos,df_empregados, df_reclamacoes)
# Escrever na camada delivery
delivered_folder = os.path.join(root, "delivery")
print("\n Escrevendo em arquivo parquet...")
write_parquet_files(spark, df_delivery, delivered_folder)
# load_dataframe_to_postgres(df_delivery, table_name, "delivery")
