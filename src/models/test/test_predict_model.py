from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092' , api_version=(0, 10, 1))

for _ in range(25):
    data = f'Kafka msg: {_}'
    future = producer.send('anomaly', data.encode('utf-8'))
    # print(f'Sending msg: {data}')
    # result = future.get(timeout=60)
    producer.flush()
# metrics = producer.metrics()
# print(metrics)