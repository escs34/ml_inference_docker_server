#producer

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

import tensorflow as tf
import tensorflow.keras as keras
from tensorflow.keras.datasets import cifar10

import numpy as np
import os

import json
from json import dumps
from json import loads

import time
from time import sleep

from random import randint


def get_data():
    data_augmentation = True

    subtract_pixel_mean = True

    num_classes = 10

    # Load the CIFAR10 data.
    (x_train, y_train), (x_test, y_test) = cifar10.load_data()

    # Input image dimensions.
    input_shape = x_train.shape[1:]

    # Normalize data.
    x_train = x_train.astype('float32') / 255
    x_test = x_test.astype('float32') / 255

    # If subtract pixel mean is enabled
    if subtract_pixel_mean:
        x_train_mean = np.mean(x_train, axis=0)
        x_train -= x_train_mean
        x_test -= x_train_mean

    print('x_train shape:', x_train.shape)
    print(x_train.shape[0], 'train samples')
    print(x_test.shape[0], 'test samples')
    print('y_train shape:', y_train.shape)

    # Convert class vectors to binary class matrices.
    y_train = keras.utils.to_categorical(y_train, num_classes)
    y_test = keras.utils.to_categorical(y_test, num_classes)

    return x_train, y_train, x_test, y_test


def main(ip_address, port):
    topic = 'cifar10'
    broker_address = ip_address + ":" + port 
    producer = KafkaProducer(acks=0, compression_type='gzip', bootstrap_servers=[broker_address], value_serializer=lambda x: dumps(x).encode('utf-8'))

    x_train, y_train, x_test, y_test = get_data()
    
    start_time = time.time()
    
    
    num_sended = 100
    for i in range(num_sended):

        if i % 100 == 0:
            print(i, 'th data sending')
            
        data = {'img' : json.dumps(x_test[i:i+1].tolist())}
        #important
        producer.send(topic, value=data, partition=randint(0,2))
        producer.flush()
    print("elapsed :", time.time() - start_time)

    producer.close()

    
    
    #consumer setting
    consumer = KafkaConsumer('reply', bootstrap_servers=[broker_address], auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group', value_deserializer=lambda x: loads(x.decode('utf-8')), consumer_timeout_ms=1000 )
    print('[begin] get consumer list')
    
    before_time = time.time()
    responsed = 0
    try:
        while(True):
            current_time = time.time()
            if current_time - before_time > 10:
                print("About 10 secs passed")
                before_time = current_time

            for message in consumer:
                print(message.value['reply'])
                num_partition, num_replied = message.value['reply']
                
                responsed += num_replied
                print('num_partition,num_replied, responsed : ', num_partition, num_replied, responsed)
                
            if responsed >= num_sended:
                print('finished')
                break

    finally:
        consumer.close()
        
    end_time = time.time()
    print("Total Process time is : ", end_time - start_time)
    
if __name__ == "__main__":
    ip_address = ""
    port = ""
    with open("ip_config.txt", "r") as f:
        ip_address = f.readline()
    with open("port.txt", "r") as f:
        port = f.readline()

    main(ip_address,port)
