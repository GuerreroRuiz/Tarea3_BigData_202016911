import time
import json
import random
from kafka import KafkaProducer

# Función para generar datos de sensores
def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "timestamp": int(time.time())
    }

# Crea un productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Bucle infinito para enviar datos de sensores
while True:
    sensor_data = generate_sensor_data()
    producer.send('sensor_data', value=sensor_data)
    print(f"Sent: {sensor_data}")
    time.sleep(1)  # Espera 1 segundo antes de enviar el siguiente dato
