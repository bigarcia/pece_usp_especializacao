
## Configuração de ambiente

### Criar um ambiente virtual para instalar todas as libs

1. Criação de um ambiente virtual:
   `python3 -m venv venv`
2. Inicialização do ambiente virtual
   `source ./venv/bin/activate`
![image](https://github.com/user-attachments/assets/2bf6032d-5190-4d7d-8681-480631361743)

### Configurar Airflow


3. Instalação do Apache Airflow:
 
   3.1.`export AIRFLOW_VERSION=2.7.1`
   
   3.2.`export PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"`
   
   3.3. `export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"`
   
   3.4. `pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"`

![image](https://github.com/user-attachments/assets/e5b66054-905b-41f4-a453-7bc571a51907)

4. Inicialização do banco

   `airflow db init`

![image](https://github.com/user-attachments/assets/21f4c9d1-49f0-495e-9e85-713c54f2c515)

5. Criação de um usuário admin

   `airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com`


6. Instalação de bibliotecas
    `pip install apache-airflow-providers-apache-spark`
![image](https://github.com/user-attachments/assets/6762f48d-37db-4150-afca-16ff3562e027)

Problemas na instalação:
![image](https://github.com/user-attachments/assets/3dec56ae-ad08-40be-a1d6-0474cc124997)

7. Inicie o scheduler (agenda e executa as tarefas)

   `airflow scheduler`
   ![image](https://github.com/user-attachments/assets/48d7421a-98f9-4160-bb73-ede3188404e0)

8. Em outro terminal, iniciamos o Webserver (oferece a interface gráfica)

   `airflow webserver --port 8080`
   ![image](https://github.com/user-attachments/assets/3f2f4b7c-265e-4a8b-aae6-89765b6b9715)


9. Acesse a interface do Airflow utilizando o usuário admin e senha admin
   
   `http://localhost:8793/`
   
   ![image](https://github.com/user-attachments/assets/6866f269-e560-46b4-8bc2-e04983bbd148)
   ![image](https://github.com/user-attachments/assets/9626725c-17a9-4419-ab49-70f625a08892)

10. Criação da pasta airflow/dags e inserção do arquivo python que cria a DAG nesse pasta
    `~airflow/dags`
    
### Configurar DataHub
1. Clonando repositório
`git clone https://github.com/datahub-project/datahub.git`

`cd datahub/docker/quickstart`

2.Instalar docker desktop
https://www.docker.com/products/docker-desktop/
Abrir o docker desktop 

3. Iniciar container do DataHub com Docker compose:

   `docker-compose -f docker-compose.quickstart.yml up`


Esse comando vai iniciar todos os serviços do DataHub, incluindo:

GMS (Graph Metadata Service) – backend do DataHub.
Frontend – interface web do DataHub.
Elasticsearch – mecanismo de busca para os metadados.
Kafka – pipeline de eventos para metadados.
MySQL – banco de dados onde os metadados são armazenados.

![image](https://github.com/user-attachments/assets/b854f798-cd6d-4438-9bc3-ed420e31261d)

3. Acessar interface Datahub, utilizando o usuário datahub e senha datahub
   `http://localhost:9002`

### Configurar Great Expectations
1. Instalar
`pip install great_expectations`
`pip show great_expectations`
![image](https://github.com/user-attachments/assets/eb963fa7-95de-4c20-85f2-657c4ddc7c48)
![image](https://github.com/user-attachments/assets/3d0bbfd8-b7e3-4c1a-8a06-8063a6b5f0d2)

Instalado em: /Users/bianca.martins/venv/lib/python3.9/site-packages

Error:
![image](https://github.com/user-attachments/assets/02bc4f15-6693-4898-b5f5-5f91d001596b)
![image](https://github.com/user-attachments/assets/13f95a2f-268a-4a9a-8c7a-def16a47129e)

3. Inicializar
`great_expectations init`

## Arquivo: `airflow_dag_with_great_expectations_and_datahub.py`

Este arquivo define um fluxo de trabalho no Airflow com as seguintes etapas/tasks:

1. **Execução do PySpark**: Primeiro, ele roda um script PySpark que você pode ajustar para seu próprio processamento de dados.
2. **Verificação de Qualidade com Great Expectations**: Depois, ele verifica se os dados estão corretos (por exemplo, se uma coluna específica existe ou se valores nulos estão onde não deveriam estar).
3. **Integração com o DataHub**: Finalmente, ele envia os metadados desse fluxo para o **DataHub**, para que você possa rastrear e monitorar o ciclo de vida dos seus dados.

### Componentes principais:

- **SparkSubmitOperator**: Este operador do Airflow envia o trabalho PySpark para ser executado.
- **PythonOperator**: Roda o código que faz a verificação de qualidade dos dados usando o **Great Expectations**.
- **DatahubEmitterOperator**: Este operador é responsável por enviar os metadados do seu pipeline para o DataHub. Link: https://registry.astronomer.io/providers/datahub/versions/latest/modules/datahubemitteroperator


### Como configurar o DataHub no Airflow:

1. No Airflow, vá em **Admin > Connections**.
2. Adicione uma nova conexão:
   - **Conn Id**: `datahub_rest_default`
   - **Conn Type**: `HTTP`
   - **Host**: URL do DataHub.
   - **Extra**: `{"authorization": "Bearer <seu_token_de_autorizacao>"}`.

### Como usar este arquivo:

1. **Coloque a DAG no Airflow**: Fizemos o upload do arquivo `ingestion.py` para a pasta onde ficam as DAGs (geralmente algo como `~/airflow/dags/`).

2. **Atualize o caminho do script PySpark**: No campo `application`, altere o caminho para o local onde está o seu script PySpark.

3. **Instale as dependências**:
   Execute os comandos abaixo para garantir que todas as bibliotecas necessárias estão instaladas:

   ```bash
   pip install great_expectations
   pip install acryl-datahub[datahub-airflow-plugin]
   ```

4. **Execute o DAG**: Depois que o DAG estiver no Airflow, você pode ativá-lo e monitorar sua execução através da interface web do Airflow.

5. **Acompanhe os metadados no DataHub**: Após a execução, você poderá ver as informações dos dados processados no DataHub, como quais datasets foram usados, o histórico de execuções, e outros detalhes importantes para monitoramento.

### Considerações:

- Certifique-se de que o Spark e o DataHub estão devidamente configurados no seu ambiente de execução.
- Verifique se as bibliotecas Great Expectations e o plugin DataHub estão instalados no ambiente onde o Airflow está rodando.

Com isso, você terá uma visão mais clara do ciclo de vida dos seus dados, tanto em termos de execução (via Airflow) quanto de metadados (via DataHub).

Bom trabalho e bons dados!
