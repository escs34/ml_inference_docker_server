#producer

from kafka import KafkaProducer
from tensorflow.keras.datasets import cifar10

import tensorflow as tf
import tensorflow.keras as keras

import numpy as np
import os

import json
from json import dumps

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
    
    start = time.time()
    
    for i in range(10):

        if i % 100 == 0:
            print(i, 'th data sending')
            
        data = {'img' : json.dumps(x_test[i:i+1].tolist())}
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
