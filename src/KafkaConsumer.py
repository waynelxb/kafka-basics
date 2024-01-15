from kafka import KafkaConsumer

KAFKA_CONSUMER_GROUP_NAME_CONS = "test-consumer-group"
KAFKA_TOPIC_NAME_CONS = "orderstopicdemo"
KAFKA_BOOTSTRAP_SERVERS_CONS = "192.168.1.222:9092"

print("Kafka Consumer Application Started ... ")

# auto_offset_reset='latest'
# auto_offset_reset='earliest'
consumer = KafkaConsumer(
    KAFKA_TOPIC_NAME_CONS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
    # value_deserializer=lambda x: loads(x.decode('utf-8')))
    value_deserializer=lambda x: x.decode('utf-8'))

for message in consumer:
    # print(dir(message))
    print(type(message))
    print("Key: ", message.key)
    message = message.value
    print("Message received: ", message)
