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


## Get stock data
ticker_symbol = "TSLA"
stock = yf.Ticker(ticker_symbol)
history_data = stock.history(period="1mo", interval="1d")
history_data_json_string = history_data.to_json(orient="table")
history_data_json_dict = json.loads(history_data_json_string)
stock_data_section_json_dict=history_data_json_dict["data"]


## Set Kafka
conf = {"bootstrap.servers": "localhost:9092",
        "client.id": socket.gethostname()
        }
producer = Producer(conf)
topic = "stock_price"



## Set schema registry
schema_registry_conf = {"url": "http://localhost:8091"}
client = SchemaRegistryClient(schema_registry_conf)

## Be careful! json element should be quoted with DOUBLE QUOTES!!!
## Space is not allowedin Key Name. "Stock Splits" should be set to "StockSplits"
stock_data_schema = """
{
    "type": "record",
    "namespace": "com.kubertenes1",
    "name": "AvroDeployment",
    "fields": [
        {"name": "Date", "type": "string"},
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


## Produce message with schema registry
for message in stock_data_section_json_dict: 
    print(message)
    producer.produce(
        topic=topic,
        value=avro_serializer(message, SerializationContext(topic, MessageField.VALUE)),
    )
    producer.flush()  # Ensure the message is sent immediately
    time.sleep(3)

