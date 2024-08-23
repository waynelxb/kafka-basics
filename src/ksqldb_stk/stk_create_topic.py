from confluent_kafka.admin import AdminClient, NewTopic
import pprint
admin_client = AdminClient({
    "bootstrap.servers": "localhost:9092"
})



topic_list = []
topic_list.append(NewTopic("idx.stock", 1, 1))
# topic_list.append(NewTopic("idx.company", 1, 1))
admin_client.create_topics(topic_list)


pprint.pprint(admin_client.list_topics().topics) 