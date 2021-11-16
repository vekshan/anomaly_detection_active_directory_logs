import json
from pickle import load
from kafka import KafkaConsumer

# clf = load("model.pkl", "rb")
# scaler = load("scaler.pkl", "rb")


consumer = KafkaConsumer(
    'anomaly',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest')

for message in consumer:
    print(message)
    # message = message.value
    # record = json.loads(message.value().decode('utf-8'))
    # data = record["data"]
    #
    # print(data)
