import argparse
import json
import logging
import sys
import time
import socket
from confluent_kafka import Producer, KafkaError

logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    handlers=[logging.FileHandler("producer.log"), logging.StreamHandler(sys.stdout)],
)


def ack_handler(err, msg):
    if err:
        logging.error({"message": "failed to deliver message", "error": str(err)})
    else:
        logging.info(
            {
                "message": "successfully produced message",
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
            }
        )


def parse_stocks(filepath):
    stocks = []
    with open(filepath) as fp:
        for i, line in enumerate(fp):
            line = line.strip()
            if i > 0 and line:  # skip header row
                # print(line)
                symbol, quote, low_day, high_day = line.split()
                # print(symbol, quote, low_day, high_day)
                stocks.append(
                    {
                        "symbol": symbol,
                        "quote": float(quote),
                        "low_day": float(low_day),
                        "high_day": float(high_day),
                    }
                )
    return stocks


def publish_quote(producer, topic, stock_quote):
    print(stock_quote)
    producer.produce(
        topic,
        key=stock_quote["symbol"],
        value=json.dumps(stock_quote),
        on_delivery=ack_handler,
    )


def publish_stocks(topic, publish_interval):
    print(topic)

    conf = {"bootstrap.servers": "localhost:9092", "client.id": socket.gethostname()}
    producer = Producer(conf)

    acme_stocks = parse_stocks(
        r"D:\\Projects\\GitHub\\kafka-basics\\src\\McQuistan_ksqldb\\data\\acme-stocks.tsv"
    )
    hooli_stocks = parse_stocks(
        r"D:\\Projects\\GitHub\\kafka-basics\\src\\McQuistan_ksqldb\\data\\hoooli-stocks.tsv"
    )

    print(len(acme_stocks))

    for i in range(len(acme_stocks)):
        acme = acme_stocks[i]
        hooli = hooli_stocks[i]
        print(acme)

        publish_quote(producer, topic, acme)
        publish_quote(producer, topic, hooli)

        producer.poll(0)

        time.sleep(3)

    producer.flush()
    logging.info({"message": "Thats all folks!"})


if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--topic', help='kafka topic to produce to')
    # parser.add_argument('--publish-interval', help='time to wait, in seconds, between publishing stocks', type=int, default=19)
    # args = parser.parse_args()

    print("Publishing stock data. Enter CTRL+C to exit.")
    while True:
        publish_stocks("stocks", 3)
        # publish_stocks(args.topic, args.publish_interval)
