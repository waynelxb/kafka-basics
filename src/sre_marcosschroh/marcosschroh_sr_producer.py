
## schema_registry.client is described on https://marcosschroh.github.io/python-schema-registry-client/client/
## However, with schema_registry.client, the schema CANNOT be found from the topic on confluent control center UI.


from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import AvroMessageSerializer

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer

kafka_broker_server_ip = "localhost"

schema_registry_url = f"http://localhost:8081"  # Replace with your Schema Registry URL
schema_registry_client = SchemaRegistryClient(schema_registry_url)

value_schema_str = {
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
    ]
}
avro_user_schema = schema.AvroSchema(value_schema_str)

avro_serializer = AvroMessageSerializer(schema_registry_client)

producer_conf = {
    'bootstrap.servers': f"localhost:9092",  # Replace with your Kafka broker(s)
}
producer = Producer(producer_conf)
string_serializer = StringSerializer('utf_8')


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


record = {"name": "Mike", "age": 30}

producer.produce(
    topic='topic_marcosschroh_schema_registry_client',
    key=string_serializer('key'),
    value=avro_serializer.encode_record_with_schema("userx", avro_user_schema, record),
    on_delivery=delivery_report
)

producer.poll(0)

producer.flush()
