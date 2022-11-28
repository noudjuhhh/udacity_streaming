"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URL = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            config={
                "bootstrap.servers": BROKER_URL,
                "compression.type": "lz4",
                "schema.registry.url": SCHEMA_REGISTRY_URL,
            },
        )

    def _topic_exists(self, client, topic_name):
        """Checks if the given topic exists"""
        return client.list_topics().topics.get(topic_name) is not None

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient(
            {"bootstrap.servers": BROKER_URL, "client.id": "create_producer_topic"}
        )

        if self._topic_exists(client, self.topic_name):
            return

        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config={
                        "cleanup.policy": "delete",
                        # "compression.type": "lz4",
                        # "confluent.value.schema.validation": True,
                        # "confluent.value.key.validation": True,
                    },
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                print(f"Created topic {self.topic_name}")
            except Exception as e:
                print(f"Failed to create topic {self.topic_name}: {e}")
                raise

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
