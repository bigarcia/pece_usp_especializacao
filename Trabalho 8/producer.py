from kafka import KafkaProducer
import json
import os

def read_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def send_to_kafka(producer, topic, data):
    producer.send(topic, value=data.encode('utf-8'))

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic = 'data_topic'
    data_dir = '/path/to/data/files/'

    for file_name in os.listdir(data_dir):
        file_path = os.path.join(data_dir, file_name)
        data = read_file(file_path)
        send_to_kafka(producer, topic, data)

    producer.flush()
    producer.close()
