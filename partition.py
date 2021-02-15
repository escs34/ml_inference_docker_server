from kafka import KafkaAdminClient
from kafka.admin import NewPartitions

topic = 'test'
bootstrap_servers = '34.220.10.239:49156'

admin_client= KafkaAdminClient(bootstrap_servers=bootstrap_servers)
topic_partitions = {}
topic_partitions[topic] = NewPartitions(total_count=3)
admin_client.create_partitions(topic_partitions)

