import json
from pickle import load
from kafka import KafkaConsumer
import numpy as np
from pickle import load
import json

with open('final_model.sav', 'rb') as f:
    model = load(f)


consumer = KafkaConsumer(
    'anomaly',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest')

for message in consumer:
    data = message.value.decode('utf-8')
    # print(data)
    data = data.split(',')
    data = [float(i) for i in data]
    # print(len(data))
    data = np.array(data)
    # # data = np.frombuffer(message.value, dtype=int)
    time = data[1]  # there seem to be an index at position 0
    total_events = data[2]  # removed user, else total_events = data[2]
    features = (data[4:]/total_events).reshape(1, -1)  # removed user else 4:
    # print(features)
    # print(len(features[0]))
    if model.predict(features)[0] == 1:
        print(f'Anomaly detected at time {str(time)}')
