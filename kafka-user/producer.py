import json
from json import dumps

from kafka import KafkaProducer
import requests

response = requests.request("GET","https://24pullrequests.com/users.json")
print(type(response))

# dumping the response into json file with beautification
data_text = response.text
parse_json = json.loads(data_text)
with open('response_producer_json.json', 'w') as file:
    json.dump(parse_json, file, indent=4)
print(type(response.json()))
# creating the kafka producer 
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x:dumps(x).encode('utf-8'))

#sending the data into kafka topic named user
for res in response.json():
    producer.send('topic_user', value=res)
    

# block until all async messages are sent
producer.flush()
