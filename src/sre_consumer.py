from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from sre_function import get_schema_from_schema_registry


schema_registry_url = "http://localhost:8091"
# schema_registry_subject = "user_schema_BACKWARD"
schema_registry_subject = "user_schema_FORWARD"
schema_version_number = 2
schema_registry_client, schema = get_schema_from_schema_registry(
    schema_registry_url, schema_registry_subject, schema_version_number
)
avro_deserializer = AvroDeserializer(schema_registry_client, schema.schema_str)

## Confluent deserializing consumer settings
deserializing_consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "daily-group",
    "auto.offset.reset": "latest",
    "value.deserializer": avro_deserializer,
}
consumer = DeserializingConsumer(deserializing_consumer_conf)


topic = "topic_" + schema_registry_subject
consumer.subscribe([topic])

## Read messages
while True:
    try:
        msg = consumer.poll(1.0)
        # print(msg)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print("Received message: {}".format(msg.value()))
    except KeyboardInterrupt:
        break
consumer.close()
