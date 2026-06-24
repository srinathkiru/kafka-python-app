import json
from kafka import KafkaConsumer

# Connect to Kafka - join 'order-processors' consumer group
consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost :9092',
    group_id='order-processors',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

print("Consumer started. Waiting for orders...")

for message in consumer:
    key = message.key
    order = message.value
    partition = message.partition
    offset = message.offset
    
    print(f"[Partition {partition} | Offset {offset}] Key: {key} | Order: {order}")
    
    # Simulate processing
    print(f"  --> Processing order {order['order_id']}: {order['quantity']}x {order['item']}")