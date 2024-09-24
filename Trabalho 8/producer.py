from kafka import KafkaProducer
import json
import os

# Configurar Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Diretório dos arquivos
directory = 'Dados/Reclamações'

# Ler e enviar cada arquivo JSON
for filename in os.listdir(directory):
    if filename.endswith(".json"):
        with open(os.path.join(directory, filename), 'r') as file:
            data = json.load(file)
            producer.send('reclamacoes', value=data)
            print(f"Enviado: {filename}")

producer.flush()
