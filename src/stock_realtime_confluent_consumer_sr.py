## To register schema to confluence control center, confluent_kafka.schema_registry must be used!!!
from confluent_kafka import Consumer
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

## Set schema registry
schema_registry_conf = {"url": "http://localhost:8091"}
schema_registry_client  = SchemaRegistryClient(schema_registry_conf)
avro_deserializer = AvroDeserializer(schema_registry_client)

## Confluent consumer settings
consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "my-group",
    "auto.offset.reset": "earliest",
    "value.deserializer": avro_deserializer,
}
consumer = DeserializingConsumer(consumer_conf)
topic="stock_realtime"
consumer.subscribe([topic])

## Read messages
while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print("Received message: {}".format(msg.value()))
    except KeyboardInterrupt:
        break
consumer.close()