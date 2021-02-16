from kafka import KafkaProducer
from json import dumps
import time
from time import sleep
from random import randint

def main(ip_address, port):
    topic = 'test'
    broker_address = ip_address + ":" + port 
    producer = KafkaProducer(acks=0, compression_type='gzip', bootstrap_servers=[broker_address], value_serializer=lambda x: dumps(x).encode('utf-8'))
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

if __name__ == "__main__":
    ip_address = ""
    port = ""
    with open("ip_config.txt", "r") as f:
        ip_address = f.readline()
    with open("port.txt", "r") as f:
        port = f.readline()

    main(ip_address,port)
