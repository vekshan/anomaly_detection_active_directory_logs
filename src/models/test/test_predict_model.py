from time import sleep
from kafka import KafkaProducer
import pandas as pd
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092', api_version=(0, 10, 1))

df = pd.read_csv("../../../data/processed/final_df.csv", engine='python')

for _, row in df.iterrows():
    data = row.values.tolist()
    data = [str(element) for element in data]
    data = ",".join(data)
    future = producer.send('anomaly', data.encode('utf-8'))
    result = future.get(timeout=60)
    producer.flush()
metrics = producer.metrics()
print(metrics)
