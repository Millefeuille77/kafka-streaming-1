from kafka import KafkaConsumer
import sys

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')
consumer.subscribe(['product-topic'])

try:
    for message in consumer:
        print(message.value.decode('utf-8'))
except KeyboardInterrupt:
    sys.exit()
