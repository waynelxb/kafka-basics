import sys

# setting parent path
sys.path.append("./src")

from sre_function import register_schema
from sre_function import delete_schema_registry_subject
from sre_function import get_schema_from_schema_registry

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
from confluent_kafka.serialization import StringSerializer


import random
import socket
import yfinance as yf
import json
import time

tickers=["AAPL","TSLA"]
# tickers=["AAPL","MSFT","AMZN","JPM","WMT","UNH","PG","JNJ","HD","KO","MRK","CVX","CRM","MCD","CSCO","IBM","AMGN",
# "AXP","VZ","CAT","GS","DIS","HON","NKE","BA","INTC","MMM","TRV",]

# print(random.choice(tickers))
all_ticker_history_data_list=[]
## Get stock data
for ticker in tickers: 
    stock = yf.Ticker(ticker)
    history_record = stock.history(period="1d", interval="1m")
    history_record_json_string = history_record.to_json(orient="table")
    history_record_json_dict = json.loads(history_record_json_string)
    history_data_json_list = history_record_json_dict["data"]
    ticker_history_data_json_list=[{"ticker": ticker, **d} for d in history_data_json_list]
    all_ticker_history_data_list=all_ticker_history_data_list+ticker_history_data_json_list
# print(all_ticker_history_data_list)


## Set Kafka
conf = {"bootstrap.servers": "localhost:9092", "client.id": socket.gethostname()}
producer = Producer(conf)
stock_topic = "stk_stock"
company_topic = "stk_company"

# Define the URL of your Schema Registry
schema_registry_url = "http://localhost:8091"
schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})


key_serializer = StringSerializer("utf_8")

schema_registry_subject = "stk_stock_min_data_schema"
schema_version_number = 1
stock_schema = get_schema_from_schema_registry(
    schema_registry_client, schema_registry_subject, schema_version_number
)


print(stock_schema.schema_str)
stock_avro_serializer = AvroSerializer(schema_registry_client, stock_schema.schema_str)


schema_registry_subject = "stk_company_schema"
schema_version_number = 1
company_schema = get_schema_from_schema_registry(
    schema_registry_client, schema_registry_subject, schema_version_number
)
print(stock_schema.schema_str)
company_avro_serializer = AvroSerializer(
    schema_registry_client, company_schema.schema_str
)


for stock_info in all_ticker_history_data_list:
    print(stock_info)
    producer.produce(
        topic=stock_topic,
        value=stock_avro_serializer(
            stock_info, SerializationContext(stock_topic, MessageField.VALUE)
        ),
    )
    company_info = {
        "ticker": ticker,
        "name": yf.Ticker(ticker).info["longName"],
        "exchange": yf.Ticker(ticker).info["exchange"]
    }
    print(company_info)
    producer.produce(
        topic=company_topic,
        value=company_avro_serializer(
            company_info, SerializationContext(company_topic, MessageField.VALUE)
        ),
    )
    producer.flush()  # Ensure the message is sent immediately
    time.sleep(10)
