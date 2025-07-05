import os
import logging
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError
from dotenv import load_dotenv

load_dotenv()

admin = KafkaAdminClient(bootstrap_servers=os.getenv('BOOTSTRAP_SERVERS'))

topic_name = os.getenv('TOPIC_NAME')
topic_partition_count = int(os.getenv('TOPIC_PARTITION_COUNT'))
topic_replica_count = int(os.getenv('TOPIC_REPLICA_COUNT'))

topics = [
    NewTopic(
        name=topic_name,
        num_partitions=topic_partition_count,
        replication_factor=topic_replica_count
    ),
    NewTopic(
        name=f"{topic_name}-short",
        num_partitions=topic_partition_count,
        replication_factor=topic_replica_count,
        topic_configs={"retention.ms": "360000"},
    )
]

config = ConfigResource(
    ConfigResourceType.TOPIC,
    topic_name,
    configs={"retention.ms": "360000"}
)

try:
    admin.create_topics(topics)
    print("Topics created")
    admin.alter_configs([config])
    print(f"Topic \"{topic_name}\" configured")
except TopicAlreadyExistsError:
    logging.warning("Topic already exist")
finally:
    admin.close()
