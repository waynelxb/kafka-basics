1. Reference
https://medium.com/@s.sadathosseini/real-time-apple-stock-prices-visualization-using-yfinance-and-streamlit-c4466d0a9b51
2. Docker
Start container defined in: D:\Projects\GitHub\docker-collection\docker-containers

Error Handling:

Once you run into error like the following, you may need to go to confluent UI and drop the topic, then restart producer and consumer. 
<!-- Traceback (most recent call last):
  File "D:\Projects\GitHub\kafka-basics\.venv\lib\site-packages\confluent_kafka\deserializing_consumer.py", line 110, in poll
    value = self._value_deserializer(value, ctx)
  File "D:\Projects\GitHub\kafka-basics\.venv\lib\site-packages\confluent_kafka\schema_registry\avro.py", line 415, in __call__        
    raise SerializationError("Unexpected magic byte {}. This message "
confluent_kafka.serialization.SerializationError: Unexpected magic byte 123. This message was not produced with a Confluent Schema Registry serializer

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "d:/Projects/GitHub/kafka-basics/src/stock_daily_confluent_consumer.py", line 37, in <module>
    msg = consumer.poll(1.0)
  File "D:\Projects\GitHub\kafka-basics\.venv\lib\site-packages\confluent_kafka\deserializing_consumer.py", line 112, in poll
    raise ValueDeserializationError(exception=se, kafka_message=msg)
confluent_kafka.error.ValueDeserializationError: KafkaError{code=_VALUE_DESERIALIZATION,val=-159,str="Unexpected magic byte 123. This message was not produced with a Confluent Schema Registry serializer"} -->