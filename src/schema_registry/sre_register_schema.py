#### MAKE SURE YOU RUN THIS py UNDER THE PROMPT: "PS D:\Projects\GitHub\kafka-basics\src\schema_registry>"

from confluent_kafka.schema_registry import SchemaRegistryClient
from sre_function import register_schema
from sre_function import delete_schema_registry_subject
from sre_function import get_schema_from_schema_registry


schema_registry_url = "http://localhost:8081"
schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})

compatibility_level = "BACKWARD"  # Choose from BACKWARD, FORWARD, FULL, NONE
schema_registry_subject = f"user_schema_{compatibility_level}"


############### ONLY FOR DELETION ################
# schema_registry_subject = "idx.stock-value"
# delete_schema_registry_subject(schema_registry_url, schema_registry_subject)
##################################################


with open(f"sre_avro_schema/{compatibility_level}/user_{compatibility_level}_v1.avsc", "r") as f:
    schema_string = f.read()

schema_id = register_schema(schema_registry_client, schema_registry_subject, schema_string, compatibility_level)
print(schema_id)

# # get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)
# sr, schema = get_schema_from_schema_registry(schema_registry_url, schema_registry_subject, 1)
# print(schema.schema_str)

