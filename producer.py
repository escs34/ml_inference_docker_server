from kafka import KafkaProducer
from json import dumps
import time
from time import sleep
from random import randint

topic = 'test'

producer = KafkaProducer(acks=0, compression_type='gzip', bootstrap_servers=['34.220.10.239:49156'], value_serializer=lambda x: dumps(x).encode('utf-8'))
start = time.time()
for i in range(8000):
    if i % 1000 == 0:
        print(i, 'th data sending')
    data = {'str' : 'cluster_test'+str(i)}
    #important
    producer.send(topic, value=data, partition=randint(0,2))
    producer.flush()
print("elapsed :", time.time() - start)

producer.close()
