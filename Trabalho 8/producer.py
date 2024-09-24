from kafka import KafkaProducer
import csv
import os

def send_to_kafka(producer, topic, data):
    producer.send(topic, value=data.encode('utf-8'))

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic = 'csv_data_topic'
    csv_dir = '/path/to/csv/files/'

    for file_name in os.listdir(csv_dir):
        if file_name.endswith(".csv"):
            file_path = os.path.join(csv_dir, file_name)
            with open(file_path, mode='r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Converter linha em string CSV
                    row_data = ",".join([f"{key}={value}" for key, value in row.items()])
                    send_to_kafka(producer, topic, row_data)

    producer.flush()
    producer.close()
