import io
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from fastavro import reader, parse_schema


with open('order.avsc', 'r') as f:
    schema = parse_schema(json.load(f))


consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    group_id='pizza-shop-group'
)


dlq_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
DLQ_TOPIC = 'dead_letter_queue'


total_price = 0.0
total_count = 0

print("Consumer started. Waiting for orders...")

for message in consumer:
    try:
        for attempt in range(3):
            try:
                
                bytes_reader = io.BytesIO(message.value)
                for record in reader(bytes_reader, schema):
                    order = record

                
                # raise ValueError("Testing DLQ mechanism")
                price = order['price']
                total_price += price
                total_count += 1
                average = total_price / total_count

                print(f"Received: {order['product']} (Rs. {price:.2f}) | New Average: Rs. {average:.2f}")

                break  

            except Exception as e:
                print(f"Error on attempt {attempt+1}: {e}")
                time.sleep(1)

                if attempt == 2:
                    print(f"!!! Permanent Failure. Sending to DLQ: {DLQ_TOPIC}")
                    dlq_producer.send(DLQ_TOPIC, message.value)

    except Exception as main_e:
        print(f"Critical System Error: {main_e}")
