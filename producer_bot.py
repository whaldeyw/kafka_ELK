import json
import time
import random
from kafka import KafkaProducer
from loguru import logger

# Настраиваем продюсера
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topics = ['user_login', 'payment', 'item_view']
users = ['alice', 'bob', 'charlie', 'dave']

logger.add("app.log", rotation="10 MB") # Локальный лог на всякий случай

logger.info("🚀 Kafka Producer запущен. Генерим события...")

while True:
    event = {
        "user": random.choice(users),
        "action": random.choice(topics),
        "value": random.randint(1, 1000),
        "timestamp": time.time()
    }
    # Отправляем в Kafka (в топик, соответствующий action)
    future = producer.send(event['action'], value=event)
    logger.info(f"Отправлено: {event}")
    print(f"Sent: {event}")
    time.sleep(2)  # Каждые 2 секунды
