from time import sleep
from json import dumps
from kafka import KafkaProducer
import pandas as pd

producer = KafkaProducer(bootstrap_servers='localhost:9092' , api_version=(0, 10, 1))

df = pd.read_csv("../../../data/processed/final_df.csv")

for _, row in df.iterrows():
    data = row.values.tobytes()
    future = producer.send('anomaly', data)
    result = future.get(timeout=60)
    producer.flush()
metrics = producer.metrics()
print(metrics)
