import json
from pickle import load
from kafka import KafkaConsumer

clf = load("model.pkl")
scaler = load("scaler.pkl")


consumer = KafkaConsumer(
    'anomaly',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    message = message.value
    record = json.loads(message.value().decode('utf-8'))
    data = record["data"]

    print(data)
