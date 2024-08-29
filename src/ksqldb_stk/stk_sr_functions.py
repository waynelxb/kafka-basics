# 3rd party library imported
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema
import requests

def delete_schema_registry_subject(schema_registry_client, schema_registry_subject):
    if schema_registry_subject in schema_registry_client.get_subjects(): 
        schema_registry_client.delete_subject(schema_registry_subject, permanent=True)
    else: 
        print(f"{schema_registry_subject} does not exist")


def register_schema(schema_registry_client, schema_registry_subject, schema_str, compatibility_level= "BACKWARD"):
    # schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
    schema_registry_client.set_compatibility(schema_registry_subject, compatibility_level)
    print(f"Compatibility level for {schema_registry_subject} set to {compatibility_level}")
    schema = Schema(schema_str, schema_type="AVRO")
    schema_id = schema_registry_client.register_schema(subject_name=schema_registry_subject, schema=schema)
    return schema_id




# def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject, version_number = None):
#     schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
#     if version_number == None: 
#         schema = schema_registry_client.get_latest_version(schema_registry_subject).schema
#     else: 
#         schema = schema_registry_client.get_version(schema_registry_subject, version_number).schema
#     return schema_registry_client, schema



def get_schema_from_schema_registry(schema_registry_client, schema_registry_subject, version_number = None):
    if version_number == None: 
        schema = schema_registry_client.get_latest_version(schema_registry_subject).schema
    else: 
        schema = schema_registry_client.get_version(schema_registry_subject, version_number).schema
    return schema





def update_schema(schema_registry_client, schema_registry_subject, schema_str):
    # schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
    versions_deleted_list = schema_registry_client.delete_subject(schema_registry_subject)
    print(f"versions of schema deleted list: {versions_deleted_list}")
    schema_id = register_schema(schema_registry_client, schema_registry_subject, schema_str)
    return schema_id


# schema_registry_url = 'http://localhost:8081'
# schema_registry_subject = f"user_schema"

# with open("avro_schema/user.avsc", "r") as f:
#     schema_string = f.read()


# schema_id = register_schema(schema_registry_url, schema_registry_subject, schema_string)
# print(schema_id)

# get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)
# schema_registry_client, schema = get_schema_from_schema_registry(schema_registry_url, schema_registry_subject, 1)
# print(schema.schema_str)

# # print("XXXXXXXXXXXXXXXXXXXXXXXXX")
# # subjects = schema_registry_client.get_subjects()
# # print(subjects)
