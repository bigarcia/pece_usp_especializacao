
# Trabalho 7


1. Criação do arquivo docker-compose.yml
2. Execução do comando abaixo para iniciar o Kafka e Zookeeper localmente.
`docker-compose up -d`
3. Criar tópico chamado reclamacoes para receber os dados:`
`docker exec -it kafka kafka-topics.sh --create --topic reclamacoes --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`
4. Instalação da biblioteca `kafka-python`:
`pip install kafka-python`
5. Criação de um código python para o produtor `producer.py`, que irá ler arquivos locais e enviar as reclamações para o Kafka
6. Instalar pyspark
  `pip install pyspark`
7. Criação de Job PySpark (consumer.py), que consome os dados do Kafka, consulta o banco de dados SQL para enriquecer as reclamações, e salva os resultados em um arquivo local
8. Criar um Container MySQL com Docker:
`docker run --name mysql-db -e MYSQL_ROOT_PASSWORD=minhasenha -d -p 3306:3306 mysql:latest`

- `-e MYSQL_ROOT_PASSWORD=minhasenha`: Define a senha do usuário root.
- `-p 3306:3306`: Mapeia a porta do MySQL (3306) no container para a porta 3306 no host.

10.Acessar o MySQL do container:
`docker exec -it mysql-db mysql -u root -p`
11. Criar Banco de Dados e tabelas:

`CREATE DATABASE trabalho7;
USE reclamacoes_db;

CREATE TABLE reclamacoes (
    id INT PRIMARY KEY AUTO_INCREMENT,
    informacao VARCHAR(255)
);`


