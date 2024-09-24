
# Trabalho 8 - Pipeline Streaming com PySpark e Kafka

### Passo a Passo
1. Instalação do docker: https://www.docker.com/
2. Instalar bibliotecas `pyspark`, `kafka-python`, `mysql-connector-python`
  `pip install pyspark kafka-python mysql-connector-python`
3. Criação do arquivo docker-compose.yml
4. Execução do comando abaixo para iniciar o Kafka e Zookeeper localmente.
`docker-compose up -d`
5. Visualizar os containers existentes
   `docker ps`
6. Acessar container do Kafka:
   `docker exec -it cb76b571aca1 bash`\
7. Baixar Kafka dentro do container:
   `curl https://archive.apache.org/dist/kafka/2.8.0/kafka_2.13-2.8.0.tgz -o kafka.tgz`

   `tar -xvzf kafka.tgz`
8. Criar tópico chamado reclamacoes para receber os dados:
  `cd kafka_2.13-2.8.0`

  `./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic reclamacoes`
9. Criação de um código python para o produtor `producer.py`, que irá ler arquivos locais e enviar as reclamações para o Kafka
10. Criação de Job PySpark (consumer.py), que consome os dados do Kafka, consulta o banco de dados SQL para enriquecer as reclamações, e salva os resultados em um arquivo local

11. Criar Banco de Dados e tabelas:

`CREATE DATABASE trabalho7;
USE reclamacoes_db;

CREATE TABLE reclamacoes (
    id INT PRIMARY KEY AUTO_INCREMENT,
    informacao VARCHAR(255)
);`

12. Executar o producer:
`python producer.py`

13. Executar o consumer:
`spark-submit consumer.py`

14. Os dados processados e enriquecidos estarão no diretório `./output`.


### Arquitetura
o produtor de dados é um script em Python que lê informações dos arquivos (.csv) de reclamações armazenados localmente, e atua como um produtor Kafka, responsável por enviar os dados lidos para um tópico específico no Kafka. O Kafka, por sua vez, é configurado em um ambiente local utilizando o Docker para disponibilizar o Apache Kafka e o Zookeeper, formando a fila de mensagens que receberá os dados enviados pelo produtor.

No Kafka, os dados são armazenados temporariamente nos tópicos, onde ficam disponíveis para serem consumidos de forma contínua pelo consumidor. Esse consumidor é implementado como um job PySpark utilizando a biblioteca "Structured Streaming", que consome os dados do tópico Kafka e, com base em uma janela de tempo (window), processa os dados em pequenos lotes.

Durante esse processamento, o PySpark faz uma requisição a um banco de dados MySQL, via JDBC, para obter informações adicionais e enriquecer as mensagens recebidas do Kafka.

Após o processo de enriquecimento, os dados tratados são salvos em arquivos no disco local no formato parquet. Todo esse fluxo, desde a ingestão dos dados até o armazenamento dos resultados processados, é orquestrado para ser executado de forma contínua e automatizada, com o job PySpark mantendo-se em execução até que seja explicitamente interrompido.
