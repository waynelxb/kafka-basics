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



## Get stock data
ticker_symbol = "TSLA"
stock = yf.Ticker(ticker_symbol)


# avro_schema = schema.AvroSchema(deployment_schema)
# schema_id = client.register("test-deployment1", avro_schema)
# print("########################")
# print(schema_id)

# # Get the data of the stock
# stock = yf.Ticker(ticker_symbol)

# # Create a matplotlib figure
# fig, ax = plt.subplots()

# # Use st.pyplot to display the plot
# plot = st.pyplot(fig)

# producer = Producer(conf)
# topic = "stock_price"

# Loop to fetch and update stock values
while True:
    # Get the historical prices for Apple stock

    history_data = stock.history(period='1d', interval='1m')


    history_data_json_string = history_data.to_json(orient="table")
    history_data_json_dict = json.loads(history_data_json_string)
    stock_data_section_json_dict=history_data_json_dict["data"]

    # historical_prices = stock.history(period='1d', interval='1m')

    # # Get the latest price and time
    # print(historical_prices)
    # latest_price = historical_prices['Close'].iloc[-1]
    # latest_time = historical_prices.index[-1].strftime('%H:%M:%S')

    # sr = SchemaRegistryClient(url="http://localhost:8091")
    # my_schema = sr.get_schema(subject='test-deployment1', version='latest')
    # # print("########################")
    # # print(my_schema.schema_id)
    message = stock_data_section_json_dict
    print(message)
    producer.produce(
        topic=topic,
        value=avro_serializer(message, SerializationContext(topic, MessageField.VALUE)),
    )
    producer.flush()  # Ensure the message is sent immediately
    time.sleep(3)

    # # fetch_and_produce_stock_price(producer=producer, topic=topic, symbol=ticker_symbol)
    # message = f'{ticker_symbol}|{latest_time}|{latest_price}'  # Combine symbol and price
    # print(message)
    # producer.produce(topic, value=message)
    # producer.flush()  # Ensure the message is sent immediately

    # time.sleep(60)

# # Run this from Terminal
# # streamlit run this.py
