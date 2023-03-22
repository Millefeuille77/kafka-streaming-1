# run kafka server and zookeper first before starting this program

from kafka import KafkaProducer
import json
import requests

#get data from dummyjson REST API
r = requests.get('https://dummyjson.com/products')
product_r = r.json()

product_data = product_r['products']

topicname = 'product-topic'

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0,10,2))

for val in product_data:
    row = str(val)
    ack = producer.send(topicname, row.encode('utf-8'))