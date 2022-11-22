from kafka import KafkaConsumer
import json
from json import loads

# creating the kafka consumer
consumer = KafkaConsumer(
    'topic_user',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8')))

# dumping the response into json file with beautification
for con in consumer:
    with open('response_consumer_json.json', 'a') as file: 
        json.dump(con.value , file, indent=4)





