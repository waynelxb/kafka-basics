import yfinance as yf
from confluent_kafka import Producer
import socket
from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import AvroMessageSerializer

import streamlit as st
import matplotlib.pyplot as plt
import time

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()
        }
ticker_symbol = 'TSLA'


client = SchemaRegistryClient(url="http://localhost:8091")
avro_message_serializer = AvroMessageSerializer(client)


deployment_schema = {
    "type": "record",
    "namespace": "com.kubertenes1",
    "name": "AvroDeployment",
    "fields": [
        {"name": "Datetime", "type": "string"},
        {"name": "Open", "type": "float"},
        {"name": 'High', "type": "float"},
        {"name": 'Low', "type": "float"},
        {"name": 'Close', "type": "float"},
        {"name": 'Volume', "type": "float", "default": 0.0},
        {"name": 'Dividends', "type": "float", "default": 0.0},
        {"name": 'StockSplits', "type": "float", "default": 0.0},
    ],
}

avro_schema = schema.AvroSchema(deployment_schema)

schema_id = client.register("test-deployment1", avro_schema)
print("########################")
print(schema_id)

# Get the data of the stock
stock = yf.Ticker(ticker_symbol)

fig, ax = plt.subplots()


producer = Producer(conf)
topic = "stock_price"

# Loop to fetch and update stock values
while True:
    # Get the historical prices for Apple stock
    historical_prices = stock.history(period='1d', interval='1m')

    # Get the latest price and time
    print(historical_prices)
    latest_price = historical_prices['Close'].iloc[-1]
    latest_time = historical_prices.index[-1].strftime('%H:%M:%S')

    # Clear the plot and plot the new data
    ax.clear()

    sr = SchemaRegistryClient(url="http://localhost:8091")
    my_schema = sr.get_schema(subject='test-deployment1', version='latest')
    # print("########################")
    # print(my_schema.schema_id)

    # fetch_and_produce_stock_price(producer=producer, topic=topic, symbol=ticker_symbol)
    message = f'{ticker_symbol}|{latest_time}|{latest_price}'  # Combine symbol and price
    print(message)

    message_encoded = avro_message_serializer.encode_record_with_schema(
    "user", deployment_schema, message)


    producer.produce(topic, value=message)
    producer.flush()  # Ensure the message is sent immediately

    time.sleep(60)

# Run this from Terminal
# streamlit run this.py
