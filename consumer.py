#customer

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

import json
from json import loads

import time
from time import sleep

import tensorflow as tf
import tensorflow.keras as keras

def main(ip_address, port, partition_number, model_path):
    topic = 'cifar10'
    broker_address = ip_address + ":" + port

    consumer = KafkaConsumer( bootstrap_servers=[broker_address], auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group', value_deserializer=lambda x: loads(x.decode('utf-8')), consumer_timeout_ms=1000 )
    print('[begin] get consumer list')

    consumer.assign([TopicPartition(topic,1)])
    
    print("model loading...")
    model = tf.keras.models.load_model(model_path)
    print("model loaded!")
    
    before_time = time.time()

    current_batch = []
    micro_batch_size = 10
    try:
        while(True):
            current_time = time.time()
            if current_time - before_time > 10:
                print("About 10 secs passed")
                before_time = current_time

            for message in consumer:
                if len(current_batch) == micro_batch_size:
                    break
                message.value['img']
                received_x = np.array(json.loads(message.value['img']))
                current_batch.append(received_x)
                
            if len(current_batch) > 0 :
                print("Current batch size : ",len(current_batch))
                predict_start = time.time()
                model.predict(current_batch)
                predict_end = time.time()
                print("Predict time : ", predict_end - predict_start)
                current_batch = []
                
            #break

    finally:
        consumer.close()

    print('[end] get consumer list')


if __name__ == "__main__":
    ip_address = ""
    with open("ip_config.txt", "r") as f:
        ip_address = f.readline()
        
    port = ""
    with open("port.txt", "r") as f:
        port = f.readline()
        
    partition_number = -1
    with open("partition_number.txt", "r") as f:
        partition_number = int(f.readline())
        
    model_path = ""
    with open("model_path.txt", "r") as f:
        model_path = f.readline()
        
    print(ip_address, port, partition_number, model_path)
    
    main(ip_address,port,partition_number, model_path)

    