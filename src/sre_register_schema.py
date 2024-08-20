from sre_function import register_schema
from sre_function import delete_schema_registry_subject
from sre_function import get_schema_from_schema_registry

subject_name = "your-subject-name"  # Replace with your subject name
compatibility_level = "FORWARD"  # Choose from BACKWARD, FORWARD, FULL, NONE

schema_registry_url = 'http://localhost:8091'
schema_registry_subject = f"user_schema_{compatibility_level}"
# schema_registry_subject = "topic_user_schema_FORWARD-value"
# delete_schema_registry_subject(schema_registry_url, schema_registry_subject)

with open(f"avro_schema/{compatibility_level}/user_{compatibility_level}_v3.avsc", "r") as f:
    schema_string = f.read()

schema_id = register_schema(schema_registry_url, schema_registry_subject, schema_string, compatibility_level)
print(schema_id)

# # get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)
# sr, schema = get_schema_from_schema_registry(schema_registry_url, schema_registry_subject, 1)
# print(schema.schema_str)

