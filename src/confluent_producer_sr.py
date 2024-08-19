from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from uuid import uuid4

# Define your Avro schema as a string
value_schema_str = """
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}
"""


# Function to convert the User object to a dictionary
def user_to_dict(user, ctx):
    print(user)
    return user


# Define the URL of your Schema Registry
schema_registry_url = 'http://localhost:8081'

# Create a SchemaRegistryClient instance
schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
registered_schema = schema_registry_client.get_latest_version("user")

print(registered_schema.schema.schema_str)

# Create a StringSerializer instance for the key
key_serializer = StringSerializer('utf_8')
# Create an AvroSerializer instance
value_serializer = AvroSerializer(schema_registry_client, value_schema_str, user_to_dict)

# Define the configuration for the SerializingProducer
producer_config = {
    'bootstrap.servers': 'localhost:9092',  # Change to your Kafka broker
    'key.serializer': key_serializer,
    'value.serializer': value_serializer
}

# Create a SerializingProducer instance
producer = SerializingProducer(producer_config)


# Define a delivery report callback function
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


# Produce a message
key = str(uuid4())
value = {"name": "Jane Doe", "age": 25}
topic = 'telemetry'

# Produce the message
producer.produce(topic=topic, key=key, value=value, on_delivery=delivery_report)
producer.flush()

print(f'Message produced to topic {topic}')
