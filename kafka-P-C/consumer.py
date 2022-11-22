from kafka import KafkaConsumer
import json
from json import loads

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))
    
for num in consumer:
    with open('response_consumer_json.json', 'w') as file:
        json.dump(num.value, file, indent=4)