
# Como executar um DAG do Airflow com PySpark, validação de dados com Great Expectations e integração com DataHub

Este guia vai te ajudar a configurar e executar um DAG no Airflow que faz três coisas importantes:
1. Roda um script PySpark.
2. Verifica a qualidade dos dados usando **Great Expectations**.
3. Envia informações sobre o seu pipeline de dados para o **DataHub**.

## Configuração de ambiente
1. Criação de um ambiente virtual:
   `python3 -m venv venv`
2. Inicialização do ambiente virtual
   `source ./venv/bin/activate`
![image](https://github.com/user-attachments/assets/2bf6032d-5190-4d7d-8681-480631361743)

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

6. Inicie o scheduler (agenda e executa as tarefas)

   `airflow scheduler`
   ![image](https://github.com/user-attachments/assets/48d7421a-98f9-4160-bb73-ede3188404e0)

7. Em outro terminal, iniciamos o Webserver (oferece a interface gráfica)

   `airflow webserver --port 8080`
   ![image](https://github.com/user-attachments/assets/3f2f4b7c-265e-4a8b-aae6-89765b6b9715)


9. Acesse a interface do Airflow:
   
   `http://localhost:8793/`
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
