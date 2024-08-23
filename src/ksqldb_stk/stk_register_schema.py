import sys

# setting parent path
sys.path.append("./src/schema_registry")
from confluent_kafka.schema_registry import SchemaRegistryClient
from stk_sr_functions import register_schema
from stk_sr_functions import delete_schema_registry_subject
from stk_sr_functions import get_schema_from_schema_registry

schema_registry_url = "http://localhost:8091"
schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})


compatibility_level = "BACKWARD"  # Choose from BACKWARD, FORWARD, FULL, NONE
schema_registry_subject = "stk_stock_min_data_schema"

delete_schema_registry_subject(schema_registry_client, schema_registry_subject)


with open(f"src\ksqldb_stk\stk_avro_schema\{schema_registry_subject}.avsc", "r") as f:
    schema_string = f.read()

schema_id = register_schema(
    schema_registry_client, schema_registry_subject, schema_string, compatibility_level
)
print(schema_id)

# # # get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)
# # sr, schema = get_schema_from_schema_registry(schema_registry_url, schema_registry_subject, 1)
# # print(schema.schema_str)
