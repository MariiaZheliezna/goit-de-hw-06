import random
import json
import time

from confluent_kafka import Producer
from configs import kafka_config
from datetime import datetime


# Ідентифікатор сенсора
sensor_id = random.randint(100, 110)

def sensor_data_generator(sensor_id):
    return { 
        "sensor_id": sensor_id, 
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
        "temperature": random.randint(25, 46), 
        "humidity": random.randint(15, 86)
        }

def delivery_report(err, msg): 
    if err is not None: 
        print(f"Delivery failed for message {msg.key()}: {err}") 
    else: 
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

producer = Producer(kafka_config)

# Імітація роботи сенсора
try:
    while True:
        data = sensor_data_generator(sensor_id)
        producer.produce('building_sensors_MZ', value=json.dumps(data).encode('utf-8'), callback=delivery_report)
        producer.poll(1)
        print(f"Відправлено: {data}")
        time.sleep(2)
except KeyboardInterrupt:
    print("Виконання закінчено...")
finally:
    producer.flush()  # Очікування завершення відправлення всіх повідомлень