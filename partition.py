from kafka import KafkaAdminClient
from kafka.admin import NewPartitions

def main(ip_address, port):
    topic = 'test'
    bootstrap_servers = ip_address + ":" + port

    admin_client= KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topic_partitions = {}
    topic_partitions[topic] = NewPartitions(total_count=3)
    admin_client.create_partitions(topic_partitions)

if __name__ == "__main__":
    ip_address = ""
    port = ""
    with open("ip_config.txt", "r") as f:
        ip_address = f.readline()
    with open("port.txt", "r") as f:
        port = f.readline()

    main(ip_address,port)
