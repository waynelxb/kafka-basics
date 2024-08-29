# 3rd party library imported
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema

# imort from constants
from wikimedia_stringified_schema import SCHEMA_STR

schema_registry_url = 'http://localhost:8081'
kafka_topic = 'wikimedia'
schema_registry_subject = f"{kafka_topic}-value"

def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)
    return sr, latest_version

def register_schema(schema_registry_url, schema_registry_subject, schema_str):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    schema = Schema(schema_str, schema_type="AVRO")
    schema_id = sr.register_schema(subject_name=schema_registry_subject, schema=schema)
    return schema_id

def update_schema(schema_registry_url, schema_registry_subject, schema_str):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    versions_deleted_list = sr.delete_subject(schema_registry_subject)
    print(f"versions of schema deleted list: {versions_deleted_list}")
    schema_id = register_schema(schema_registry_url, schema_registry_subject, schema_str)
    return schema_id


schema_id = register_schema(schema_registry_url, schema_registry_subject, SCHEMA_STR)
print(schema_id)

sr, latest_version = get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)
print(latest_version.schema.schema_str)

# print("XXXXXXXXXXXXXXXXXXXXXXXXX")
# subjects = sr.get_subjects()
# print(subjects)
