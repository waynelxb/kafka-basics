import os

# Kafka Cluster Host
config = {
    'bootstrap.servers': 'localhost:39092,localhost:39093,localhost:39094'
}
# Kafka Schema Host
srConfig = {
    'url': 'http://localhost:8282'
}
# ksqlDB Host
ksqlConfig = {
    'url': 'http://localhost:9088',
    'mode': 'earliest'  # earliest/latest
}
# MongoDB Host
mongoConfig = {
    'url': 'mongodb://localhost:27017'
}
# Spark Host - Prepare this for next study case
sparkMasterConfig = {
    'url': 'spark://192.168.1.4:7077'
}
rootDirectory = os.path.dirname(os.path.abspath(__file__))  # This is Project Root
apiKey = '[your-goapi-key]'  # GoAPI Api-Key