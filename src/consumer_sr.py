import json
from kafka import KafkaConsumer
from schema_registry.client import SchemaRegistryClient

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient('http://localhost:8091')

# Create a Kafka consumer
consumer = KafkaConsumer('newtopic',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest',
                         group_id='my-group',
                         value_deserializer=lambda x: schema_registry_client.decode(x))

# Consume messages from the topic
for message in consumer:
    # Decode the message using the Schema Registry client
    value = schema_registry_client.decode(message.value)

    # Print the message
    print(value)