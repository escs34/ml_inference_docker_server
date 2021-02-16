#customer

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

import json
from json import loads
from json import dumps

import time
from time import sleep

import tensorflow as tf
import tensorflow.keras as keras

import numpy as np

def main(ip_address, port, partition_number, model_path, micro_batch_size):
    #broker configure
    topic = 'cifar10'
    broker_address = ip_address + ":" + port

    #consumer setting
    consumer = KafkaConsumer( bootstrap_servers=[broker_address], auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group', value_deserializer=lambda x: loads(x.decode('utf-8')), consumer_timeout_ms=1000 )
    print('[begin] get consumer list')

    consumer.assign([TopicPartition(topic,partition_number)])
    
    print("model loading...")
    model = tf.keras.models.load_model(model_path)
    print("model loaded!")
    
    
    ##for reply producer
    producer = KafkaProducer(acks=0, compression_type='gzip', bootstrap_servers=[broker_address], value_serializer=lambda x: dumps(x).encode('utf-8'))
    
    
    
    before_time = time.time()

    current_batch = np.array([])
    
    try:
        while(True):
            current_time = time.time()
            if current_time - before_time > 10:
                print("About 10 secs passed")
                before_time = current_time

            for message in consumer:
                received_x = np.array(json.loads(message.value['img']))
                #print("received_x.shape ", received_x.shape)
                if len(current_batch) == 0 :
                    current_batch = received_x
                else:
                    current_batch = np.concatenate((current_batch, received_x))
                #print("current_batch shape ", current_batch.shape)
                #print("current_batch len ", len(current_batch))
                
                if len(current_batch) == micro_batch_size:
                    break
                    
            if len(current_batch) > 0 :
                print("Current batch shape For Predict : ", current_batch.shape)
                predict_start = time.time()
                model.predict(current_batch)
                predict_end = time.time()
                print("Predict time : ", predict_end - predict_start)

                
                #reply
                data = {'reply' : (partition_number, len(current_batch))}

                producer.send('reply', value=data)
                producer.flush()

                current_batch = np.array([])


    finally:
        consumer.close()
        producer.close()
        
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
        
    micro_batch_size = 1
    with open("micro_batch_size.txt", "r") as f:
        micro_batch_size = int(f.readline())
        
    print(ip_address, port, partition_number, model_path, micro_batch_size)
    
    main(ip_address,port,partition_number, model_path, micro_batch_size)

    