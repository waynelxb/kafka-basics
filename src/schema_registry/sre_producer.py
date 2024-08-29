from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from uuid import uuid4
from sre_function import get_schema_from_schema_registry


############# INPUT AREA ####
schema_registry_url = "http://localhost:8081"
schema_registry_subject = "user_schema_BACKWARD"
schema_version_number = 1
#############################


schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})

schema = get_schema_from_schema_registry(
    schema_registry_client, schema_registry_subject, schema_version_number
)


# Function to convert the User object to a dictionary
def user_to_dict(user, ctx):
    print(user)
    return user


# Create a StringSerializer instance for the message.key
key_serializer = StringSerializer("utf_8")
# Create an AvroSerializer instance for the message.value
value_serializer = AvroSerializer(
    schema_registry_client, schema.schema_str, user_to_dict
)


# Define the configuration for the SerializingProducer
producer_config = {
    "bootstrap.servers": "localhost:9092",  # Change to your Kafka broker
    "key.serializer": key_serializer,
    "value.serializer": value_serializer,
}

# Create a SerializingProducer instance
producer = SerializingProducer(producer_config)


# Define a delivery report callback function
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# Set topic, message.key and message.value
topic = "topic_" + schema_registry_subject
key = str(uuid4())
value = {"name": "Jane Doe1", "age": 25}

# Produce the message
producer.produce(topic=topic, key=key, value=value, on_delivery=delivery_report)
producer.flush()

print(f"Message produced to topic {topic}")
