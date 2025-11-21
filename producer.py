import time
import random
import json
import io
from kafka import KafkaProducer
from fastavro import writer, parse_schema


with open('order.avsc', 'r') as f:
    schema = parse_schema(json.load(f))


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
topic_name = 'orders'

print("Producer started. Sending orders...")

try:
    while True:
        
        order = {
            "orderId": str(random.randint(1000, 9999)),
            "product": random.choice(["Laptop", "Mouse", "Keyboard", "Monitor"]),
            "price": round(random.uniform(50.0, 500.0), 2)
        }

        
        bytes_writer = io.BytesIO()
        writer(bytes_writer, schema, [order])
        raw_bytes = bytes_writer.getvalue()

        
        producer.send(topic_name, raw_bytes)
        print(f"Sent: {order['product']} for Rs. {order['price']:.2f} | ID: {order['orderId']}")

        time.sleep(2)

except KeyboardInterrupt:
    print("Stopping producer...")
    producer.close()
