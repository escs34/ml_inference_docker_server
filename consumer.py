from kafka import KafkaConsumer
from json import loads
import time
from time import sleep

def main(ip_address, port, partition_number):
    topic = 'test'
    broker_address = ip_address + ":" + port

    consumer = KafkaConsumer( topic, bootstrap_servers=[broker_address], auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group', value_deserializer=lambda x: loads(x.decode('utf-8')), consumer_timeout_ms=1000 )
    print('[begin] get consumer list')

    consumer.assign([TopicPartition(topic,partition_number])

    before_time = time.time()

    try:
        while(True):
            current_time = time.time()
            if current_time - before_time > 10:
                print("About 10 secs passed After last message is consumed.")
                before_time = current_time

            for message in consumer:
                print("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s" % ( message.topic, message.partition, message.offset, message.key, message.value ))



            before_time = time.time()
    
    finally:
        consumer.close()

    print('[end] get consumer list')


if __name__ == "__main__":
    ip_address = ""
    port = ""
    partition_number = -1
    with open("ip_config.txt", "r") as f:
        ip_address = f.readline()
    with open("port.txt", "r") as f:
        port = f.readline()
    with open("partition_number.txt", "r") as f:
        partition_number = f.readline()

    main(ip_address,port,partition_number)
