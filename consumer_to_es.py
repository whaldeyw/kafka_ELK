import json
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from loguru import logger
import time

# 1. Подключаемся к Kafka
try:
    consumer = KafkaConsumer(
        'user_login', 'payment', 'item_view',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='python-consumer-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    logger.success("✅ Connected to Kafka")
except Exception as e:
    logger.error(f"❌ Cannot connect to Kafka: {e}")
    exit(1)

# 2. Подключаемся к Elasticsearch (для версии 7.x)
try:
    es = Elasticsearch(
        ['http://localhost:9200'],
        timeout=30,
        max_retries=3,
        retry_on_timeout=True
    )

    if es.ping():
        logger.success("✅ Connected to Elasticsearch")
        info = es.info()
        logger.info(f"Elasticsearch version: {info['version']['number']}")
    else:
        logger.error("❌ Cannot connect to Elasticsearch")
        exit(1)
except Exception as e:
    logger.error(f"❌ Elasticsearch connection error: {e}")
    exit(1)

logger.info("👂 Consumer слушает Kafka и пишет в Elastic...")

actions = []
message_count = 0
batch_size = 50  # Уменьшим размер пачки для начала

try:
    for message in consumer:
        event = message.value
        event['kafka_topic'] = message.topic
        event['kafka_partition'] = message.partition
        event['kafka_offset'] = message.offset
        event['@timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())

        message_count += 1
        logger.debug(f"[{message_count}] {message.topic}: {event.get('user')}")

        # Отправляем каждый документ по отдельности (для простоты)
        try:
            res = es.index(index="app-events", body=event)
            logger.debug(f"  -> Document ID: {res['_id']}")
        except Exception as e:
            logger.error(f"Error indexing document: {e}")

        # Каждые 10 сообщений показываем статус
        if message_count % 10 == 0:
            logger.info(f"Processed {message_count} messages")

except KeyboardInterrupt:
    logger.info("🛑 Stopped by user")
finally:
    consumer.close()
    logger.info(f"👋 Consumer closed. Total messages: {message_count}")