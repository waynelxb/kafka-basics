## To register schema to confluence control center, confluent_kafka.schema_registry must be used!!!
from confluent_kafka import Consumer
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import yfinance as yf

## Set schema registry
schema_registry_conf = {"url": "http://localhost:8081"}
schema_registry_client  = SchemaRegistryClient(schema_registry_conf)
avro_deserializer = AvroDeserializer(schema_registry_client)

## Confluent deserializing consumer settings
deserializing_consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "daily-group",
    "auto.offset.reset": "latest",
    "value.deserializer": avro_deserializer,
}
consumer = DeserializingConsumer(deserializing_consumer_conf)

## Confluent plain consumer settings
# plain_consumer_conf = {"bootstrap.servers": "localhost:9092",
#         "group.id": "daily-group",
#         "auto.offset.reset": "earliest",
#         "enable.auto.commit": "false",
#         "auto.offset.reset": "earliest"}
# consumer = Consumer(plain_consumer_conf)


topic="stock_daily"
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