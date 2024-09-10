
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow_provider_datahub.operators.datahub import DatahubEmitterOperator
from datetime import datetime, timedelta
import great_expectations as ge
from great_expectations.dataset import SparkDFDataset
from pyspark.sql import SparkSession

# Definir parâmetros padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir o DAG
dag = DAG(
    'pyspark_job_with_data_quality_and_datahub',
    default_args=default_args,
    description='PySpark job with Data Quality checks using Great Expectations and DataHub integration',
    schedule_interval=timedelta(days=1),
)

# Função de validação de dados usando Great Expectations
def validate_data():
    # Inicializar sessão Spark
    spark = SparkSession.builder.appName('GreatExpectationsValidation').getOrCreate()

    # Ler dados
    df = spark.read.csv('Dados', header=True)

    # Aplicar Great Expectations
    ge_df = SparkDFDataset(df)

    # Definir expectativas de dados
    ge_df.expect_column_to_exist("coluna_esperada")
    ge_df.expect_column_values_to_not_be_null("coluna_esperada")
    ge_df.expect_column_values_to_be_in_set("coluna_categoria", ["valor1", "valor2", "valor3"])

    # Executar a validação
    validation_result = ge_df.validate()

    # Verificar se a validação foi bem-sucedida
    if not validation_result["success"]:
        raise ValueError("Os dados não passaram na validação de qualidade.")
    else:
        print("Validação de dados concluída com sucesso.")

# Tarefa para submeter o job PySpark
spark_job = SparkSubmitOperator(
    task_id='spark_submit_local_job',
    application='Trabalho5/exercício_3.py',  # Atualize com o caminho do script PySpark
    conn_id=None,  # Modo local
    total_executor_cores=2,
    executor_memory='2g',
    driver_memory='1g',
    num_executors=2,
    dag=dag,
)

# Tarefa de validação de dados
data_quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data,
    dag=dag,
)

# Tarefa de emissão para o DataHub
datahub_emit_task = DatahubEmitterOperator(
    task_id='emit_metadata_to_datahub',
    datahub_rest_conn_id='datahub_rest_default',  # ID da conexão configurada no Airflow
    entity_urn="urn:li:dataset:(urn:li:dataPlatform:spark,exemplo_dataset,PROD)",  # Modifique com seu dataset real
    payload={},
    dag=dag,
)

# Definir a ordem das tarefas
spark_job >> data_quality_task >> datahub_emit_task
