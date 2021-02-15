from kafka import KafkaConsumer
from json import loads
import time
def main(ip_address, port):
    broker_address = ip_address + ":" + port
    consumer = KafkaConsumer( 'test', bootstrap_servers=[broker_address], auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group', value_deserializer=lambda x: loads(x.decode('utf-8')), consumer_timeout_ms=1000 )
    print('[begin] get consumer list')
    before_time = time.time()
    while(True):
        try:
            current_time = time.time()
            if current_time - before_time > 10:
                print("About 10 secs passed")
                before_time = current_time
            for message in consumer:
                print("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s" % ( message.topic, message.partition, message.offset, message.key, message.value ))
    
        finally:
            consumer.close()
    print('[end] get consumer list')
if __name__ == "__main__":
    ip_address = ""
    port = ""
    with open("ip_config.txt", "r") as f:
        ip_address = f.readline()
    with open("port.txt", "r") as f:
        port = f.readline()
    main(ip_address,port)
