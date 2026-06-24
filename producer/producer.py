import time
import json
import random
from kafka import KafkaProducer

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',       # 'kafka' = service name in docker-compose
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

items = ['laptop', 'phone', 'tablet', 'monitor', 'keyboard', 'mouse']
order_id = 1

print("Producer started. Sending orders every 2 seconds...")

while True:
    order = {
        'order_id': order_id,
        'item': random.choice(items),
        'quantity': random.randint(1, 5),
        'status': 'NEW'
    }
    key = f"user-{random.randint(1, 3)}"   # 3 users, messages keyed by user
    
    producer.send('orders', key=key, value=order)
    print(f"Sent: [{key}] {order}")
    
    order_id += 1
    time.sleep(2)