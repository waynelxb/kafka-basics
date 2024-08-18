import pandas as pd
from kafka import KafkaProducer
from json import dumps
import time

KAFKA_TOPIC_NAME_CONS = "orderstopicdemo"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:29092"
# print("kafka started")
kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                               value_serializer=lambda x: dumps(x).encode('utf-8'))
###########################
# # Get message from a file.
############################
file_path = r"D:\Projects\GitHub\kafka-basics\data\orders.csv"
orders_pd_df = pd.read_csv(file_path)
# orders_pd_df['date_time'] = pd.str(Timestamp("now"))
orders_pd_df['date_time'] = str(pd.Timestamp("now"))
# print(orders_pd_df)
# print(orders_pd_df.head(1))
orders_list = orders_pd_df.to_dict(orient='records')
# print(orders_list[0])
for order in orders_list:
    message = order
    print("Message to be sent:", message)
    kafka_producer.send(KAFKA_TOPIC_NAME_CONS, message)
    time.sleep(0.1)
