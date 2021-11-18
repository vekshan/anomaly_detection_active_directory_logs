import json
from pickle import load
from kafka import KafkaConsumer
import numpy as np
from pickle import load

with open('final_model.sav', 'rb') as f:
    model = load(f)


consumer = KafkaConsumer(
    'anomaly',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest')

for message in consumer:
    data = np.frombuffer(message.value, dtype=int)
    time = data[0]
    total_events = data[2]
    features = (data[4:]/total_events).reshape(1,-1)
    print(features)
    print (model.predict(X)[0])
    if model.predict(features)[0] == 1:
        print(f'Anomaly detected at time {str(time)}')


