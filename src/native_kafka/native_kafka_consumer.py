from kafka import KafkaConsumer
KAFKA_CONSUMER_GROUP_NAME_CONS = "test-consumer-group"
KAFKA_TOPIC_NAME_CONS = "orderstopicdemo"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

if __name__ == "__main__":
    print("Kafka Consumer Application Started ... ")
    try:
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
    except Exception as ex:
        print("Failed to read kafka message.")
        print(ex)
