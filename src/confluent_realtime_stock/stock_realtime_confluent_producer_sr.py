## To register schema to confluence control center, confluent_kafka.schema_registry must be used!!!
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
import os
import socket
import yfinance as yf
import json
import time

## Set Kafka
conf = {"bootstrap.servers": "localhost:9092",
        "client.id": socket.gethostname()
        }
producer = Producer(conf)
# topic for minute level realtime record
topic = "stock_realtime"

## Set schema registry
schema_registry_conf = {"url": "http://localhost:8091"}
client = SchemaRegistryClient(schema_registry_conf)

## Be careful! json element should be quoted with DOUBLE QUOTES!!!
## Space is not allowedin Key Name. "Stock Splits" should be set to "StockSplits"
## For the minute level record, Datetime is the time field name
stock_data_schema = """
{
    "type": "record",
    "namespace": "com.kubertenes1",
    "name": "AvroDeployment",
    "fields": [
        {"name": "Datetime", "type": "string"},
        {"name": "Open", "type": "float"},
        {"name": "High", "type": "float"},
        {"name": "Low", "type": "float"},
        {"name": "Close", "type": "float"},
        {"name": "Volume", "type": "float", "default": 0.0},
        {"name": "Dividends", "type": "float", "default": 0.0},
        {"name": "StockSplits", "type": "float", "default": 0.0}
    ]
}
"""
avro_serializer = AvroSerializer(client, stock_data_schema)

## Get stock data
ticker_symbol = "TSLA"
stock = yf.Ticker(ticker_symbol)

while True:
    # Get the current day market (not pre/post market) price every minute in realtime 
    history_data = stock.history(period='1d', interval='1m')
    history_data_json_string = history_data.to_json(orient="table")
    history_data_json_dict = json.loads(history_data_json_string)
    stock_data_section_json_dict=history_data_json_dict["data"]
    # Get the last minute stock message in json dict
    message = stock_data_section_json_dict[-1]
    print(message)
    producer.produce(
        topic=topic,
        value=avro_serializer(message, SerializationContext(topic, MessageField.VALUE)),
    )
    producer.flush()  # Ensure the message is sent immediately
    time.sleep(60)



