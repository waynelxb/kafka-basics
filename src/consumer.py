from confluent_kafka import Consumer, KafkaError

conf = {'bootstrap.servers': '192.168.1.200:9092',
        'group.id': 'foo',
        'auto.offset.reset': 'smallest'}
consumer = Consumer(conf)

# Subscribe to the Kafka topic
consumer.subscribe(["stock_price"])

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f'Error while consuming: {msg.error()}')
        else:
            # Parse the received message
            value = msg.value().decode('utf-8')
            symbol, time, price = value.split('|')
            print(f'Received {symbol} at {time} with price {price}')

except KeyboardInterrupt:
    pass
finally:
    # Close the consumer gracefully
    consumer.close()
