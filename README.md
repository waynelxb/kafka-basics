1. Reference
realtime stock example:
https://medium.com/@s.sadathosseini/real-time-apple-stock-prices-visualization-using-yfinance-and-streamlit-c4466d0a9b51
wikimedia example:
https://medium.com/@mrugankray/create-avro-producer-for-kafka-using-python-f9029d9b2802


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


3. Schema evolution
sre is a project about schema evolution. 
The default compatibility level of confluent is BACKWARD. 
It means using the new schema can read data produced with the last schema. 
It means the new schema can have less fields than the last schema (fields in older schema can be removed from new schema). Additional optional fields can be added to new schema. Such additional fields must have default value. 
Test: 
  Step 1: register user_BACKWARD_v1, then v2, then v3 with sre_register_schema.py
  Setp 2: run sre_producer.py with set schema_version_number = 1, 
          run sre_consumer.py with set schema_version_number = 1, then run it with schema_version_number = 2 or 3. All these scenarios should work. 
  BACKWARD is good for updating schema on consumer first. 

For BACKWARD, always try to apply the updated schema on consumer side first.
For FORWARD, always try to apply the updated schema on producer side first.
All in all the key thing is to set default value properly for the schema evolution.
