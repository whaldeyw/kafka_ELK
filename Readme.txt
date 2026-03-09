# 📚 Полная инструкция: ELK + Kafka + Nginx Stack (Версия 2.0 — Кластер из 2 нод)

## 📋 Схема взаимодействия компонентов

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────────┐     ┌─────────────┐
│   Nginx     │────▶│   Logstash   │────▶│   Elasticsearch     │◀────│   Kibana    │
│ (Web Server)│     │  (Parser)    │     │  Cluster (2 ноды)   │────▶│(Visualization)
└─────────────┘     └──────────────┘     └─────────────────────┘     └─────────────┘
       │                    ▲                           ▲
       │                    │                           │
       ▼                    │                           │
┌─────────────┐     ┌──────────────┐                   │
│   File      │     │    Kafka     │───────────────────┘
│ access.log  │────▶│  (Queue)     │
└─────────────┘     └──────────────┘
                           ▲
                           │
                    ┌─────────────┐
                    │   Python    │
                    │  Producer   │
                    └─────────────┘
```

## 🔄 Три потока данных (с репликацией!)

### Поток 1: Nginx → Logstash → Elasticsearch
- Nginx пишет логи в `/var/log/nginx/access.log`
- Logstash читает файл, парсит строки в JSON
- Logstash отправляет в Elasticsearch
- **Репликация:** данные автоматически копируются на вторую ноду

### Поток 2: Python → Kafka → Logstash → Elasticsearch
- Python-продюсер генерирует события
- Kafka хранит события в топиках (`user_login`, `payment`, `item_view`)
- Logstash читает из Kafka, преобразует и отправляет в Elasticsearch
- **Репликация:** данные дублируются между нодами

### Поток 3: Python → Kafka → Python → Elasticsearch (альтернативный)
- Python-продюсер → Kafka
- Python-консьюмер читает из Kafka и пишет напрямую в Elasticsearch
- **Репликация:** данные дублируются между нодами

---

## 🚀 Пошаговая инструкция развертывания

### Предварительные требования

```bash
# Проверка установленного ПО
docker --version
docker-compose --version
python3 --version
pip3 --version

# Рекомендуемые ресурсы (для 2 нод)
# - RAM: 8-10 GB свободно
# - CPU: 4+ ядра
# - Disk: 20+ GB свободно
```

---

### Шаг 1: Создание структуры папок

```bash
# Создаем главную директорию проекта
mkdir -p ~/projects/elk-kafka-stack
cd ~/projects/elk-kafka-stack

# Создаем поддиректории
mkdir -p logstash/pipeline nginx nginx/html python
```

---

### Шаг 2: Docker Compose файлы

#### Файл `docker-compose-elk.yml` (ELK кластер + Nginx)

```yaml
version: '3.8'
services:
  elasticsearch1:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.1
    container_name: elastic-sandbox-1
    environment:
      - cluster.name=docker-cluster
      - node.name=node-1
      - discovery.seed_hosts=elasticsearch2
      - cluster.initial_master_nodes=node-1,node-2
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
      - xpack.security.enabled=false
      - xpack.security.enrollment.enabled=false
      - xpack.monitoring.collection.enabled=false
      - ingest.geoip.downloader.enabled=false
      - cluster.routing.allocation.disk.watermark.low=95%
      - cluster.routing.allocation.disk.watermark.high=97%
      - cluster.routing.allocation.disk.watermark.flood_stage=98%
    ports:
      - "9200:9200"
    volumes:
      - elastic-data-1:/usr/share/elasticsearch/data
    networks:
      - elk-net

  elasticsearch2:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.1
    container_name: elastic-sandbox-2
    environment:
      - cluster.name=docker-cluster
      - node.name=node-2
      - discovery.seed_hosts=elasticsearch1
      - cluster.initial_master_nodes=node-1,node-2
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
      - xpack.security.enabled=false
      - xpack.security.enrollment.enabled=false
      - xpack.monitoring.collection.enabled=false
      - ingest.geoip.downloader.enabled=false
      - cluster.routing.allocation.disk.watermark.low=95%
      - cluster.routing.allocation.disk.watermark.high=97%
      - cluster.routing.allocation.disk.watermark.flood_stage=98%
    ports:
      - "9201:9200"  # Вторая нода на другом порту
    volumes:
      - elastic-data-2:/usr/share/elasticsearch/data
    networks:
      - elk-net

  kibana:
    image: docker.elastic.co/kibana/kibana:8.11.1
    container_name: kibana-sandbox
    ports:
      - "5601:5601"
    environment:
      - ELASTICSEARCH_HOSTS=http://elasticsearch1:9200,http://elasticsearch2:9200
      - XPACK_SECURITY_ENABLED=false
    depends_on:
      - elasticsearch1
      - elasticsearch2
    networks:
      - elk-net

  logstash:
    image: docker.elastic.co/logstash/logstash:7.17.15
    container_name: logstash-sandbox
    volumes:
      - ./logstash/pipeline:/usr/share/logstash/pipeline:ro
      - nginx-logs:/usr/share/logstash/nginx
    ports:
      - "5000:5000"
      - "9600:9600"
    environment:
      - LS_JAVA_OPTS=-Xms256m -Xmx256m
      - XPACK_MONITORING_ENABLED=false
    depends_on:
      - elasticsearch1
      - elasticsearch2
    networks:
      - elk-net

  nginx:
    image: nginx:latest
    container_name: nginx-sandbox
    ports:
      - "8080:80"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
      - ./nginx/html:/usr/share/nginx/html:ro
      - nginx-logs:/var/log/nginx
    command: >
      sh -c "rm -f /var/log/nginx/access.log /var/log/nginx/error.log &&
             touch /var/log/nginx/access.log /var/log/nginx/error.log &&
             chmod 666 /var/log/nginx/access.log /var/log/nginx/error.log &&
             nginx -g 'daemon off;'"
    networks:
      - elk-net

networks:
  elk-net:
    driver: bridge

volumes:
  elastic-data-1:
  elastic-data-2:
  nginx-logs:
```

#### Файл `docker-compose-kafka.yml` (Kafka отдельно)

```yaml
version: '3.8'
services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka-sandbox
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes:
      - kafka-data:/var/lib/kafka/data
    networks:
      - kafka_elastic_elk-net

networks:
  kafka_elastic_elk-net:
    external: true
    name: kafka_elastic_elk-net

volumes:
  kafka-data:
```

---

### Шаг 3: Конфигурационные файлы (без изменений)

#### Nginx: `nginx/nginx.conf`

```nginx
events {
    worker_connections 1024;
}

http {
    access_log /var/log/nginx/access.log;
    error_log /var/log/nginx/error.log;
    
    server {
        listen 80;
        location / {
            root /usr/share/nginx/html;
            index index.html;
        }
    }
}
```

#### Nginx HTML: `nginx/html/index.html`

```html
<!DOCTYPE html>
<html>
<head><title>Test Page</title></head>
<body><h1>Hello from Nginx!</h1></body>
</html>
```

#### Logstash Nginx: `logstash/pipeline/nginx.conf`

```ruby
input {
  file {
    path => "/usr/share/logstash/nginx/access.log"
    start_position => "beginning"
    sincedb_path => "/dev/null"
    tags => ["nginx-access"]
  }
}

filter {
  grok {
    match => { "message" => "%{IPORHOST:clientip} - %{USER:ident} \[%{HTTPDATE:timestamp}\] \"%{WORD:method} %{URIPATHPARAM:request} HTTP/%{NUMBER:httpversion}\" %{NUMBER:response} %{NUMBER:bytes} \"%{DATA:referrer}\" \"%{DATA:agent}\"" }
  }
  date {
    match => [ "timestamp", "dd/MMM/yyyy:HH:mm:ss Z" ]
    target => "@timestamp"
  }
  mutate {
    remove_field => ["timestamp", "ident", "message"]
  }
}

output {
  elasticsearch {
    hosts => ["elasticsearch1:9200", "elasticsearch2:9200"]
    index => "nginx-logs-%{+YYYY.MM.dd}"
  }
}
```

#### Logstash Kafka: `logstash/pipeline/kafka.conf`

```ruby
input {
  kafka {
    bootstrap_servers => "kafka-sandbox:9092"
    topics => ["user_login", "payment", "item_view"]
    codec => json
    group_id => "logstash-group"
    auto_offset_reset => "earliest"
    enable_auto_commit => true
    session_timeout_ms => 30000
    heartbeat_interval_ms => 10000
    tags => ["from-kafka"]
  }
}

output {
  if "from-kafka" in [tags] {
    elasticsearch {
      hosts => ["elasticsearch1:9200", "elasticsearch2:9200"]
      index => "kafka-events-%{+YYYY.MM.dd}"
    }
    stdout { codec => rubydebug }
  }
}
```

---

### Шаг 4: Python скрипты (без изменений)

#### Продюсер: `python/producer_bot.py`

```python
import json
import time
import random
from kafka import KafkaProducer
from loguru import logger

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topics = ['user_login', 'payment', 'item_view']
users = ['alice', 'bob', 'charlie', 'dave']

logger.info("🚀 Kafka Producer запущен")

while True:
    event = {
        "user": random.choice(users),
        "action": random.choice(topics),
        "value": random.randint(1, 1000),
        "timestamp": time.time()
    }
    producer.send(event['action'], value=event)
    logger.info(f"Sent: {event}")
    time.sleep(2)
```

#### Консьюмер: `python/consumer_to_es.py`

```python
import json
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from loguru import logger

# Kafka consumer
consumer = KafkaConsumer(
    'user_login', 'payment', 'item_view',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='python-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Elasticsearch connection (версия 7.x)
es = Elasticsearch(['http://localhost:9200'])

if es.ping():
    logger.success("✅ Connected to Elasticsearch")
else:
    logger.error("❌ Cannot connect to Elasticsearch")
    exit(1)

logger.info("👂 Consumer started...")

for message in consumer:
    event = message.value
    event['kafka_topic'] = message.topic
    logger.info(f"Received: {event}")
    
    es.index(index="app-events", body=event)
```

---

### Шаг 5: Запуск стека

```bash
# 1. Создаем общую сеть
docker network create kafka_elastic_elk-net 2>/dev/null || true

# 2. Запускаем ELK кластер
cd ~/projects/elk-kafka-stack
docker-compose -f docker-compose-elk.yml up -d

# 3. Проверяем запуск
docker ps

# 4. Запускаем Kafka
docker-compose -f docker-compose-kafka.yml up -d

# 5. Проверяем кластер Elasticsearch
curl -s "http://localhost:9200/_cluster/health?pretty"
# Должно быть: "status" : "green", "number_of_nodes" : 2

# 6. Проверяем все контейнеры
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

---

### Шаг 6: Проверка работоспособности

```bash
# 1. Проверка Elasticsearch
curl http://localhost:9200

# 2. Проверка Kibana
# http://localhost:5601

# 3. Проверка Nginx
curl http://localhost:8080

# 4. Генерация трафика для Nginx
for i in {1..100}; do
    curl -s http://localhost:8080/ > /dev/null
    echo -n "."
    sleep 0.1
done
echo ""

# 5. Проверка индексов Elasticsearch
curl -s "http://localhost:9200/_cat/indices?v" | grep -E "nginx|kafka|app"

# 6. Запуск Python продюсера
cd python
python producer_bot.py &
# (Ctrl+C для остановки)

# 7. Запуск Python консьюмера
python consumer_to_es.py &
```

---

### Шаг 7: Мониторинг кластера и шардов

```bash
# 1. Здоровье кластера
curl -s "http://localhost:9200/_cluster/health?pretty"

# 2. Список нод
curl -s "http://localhost:9200/_cat/nodes?v"

# 3. Распределение шардов
curl -s "http://localhost:9200/_cat/shards?v" | grep -E "app|nginx|kafka"

# 4. Размер сегментов
curl -s "http://localhost:9200/_cat/segments?v" | head -20

# 5. Использование памяти
curl -s "http://localhost:9200/_nodes/stats/jvm" | grep -E "heap_used_percent|node_name"
```

---

### Шаг 8: Настройка репликации

```bash
# Включаем репликацию для всех индексов (по 1 реплике)
curl -X PUT "http://localhost:9200/_settings" -H 'Content-Type: application/json' -d'
{
  "index": {
    "number_of_replicas": 1
  }
}'

# Проверяем распределение шардов
watch -n 2 'curl -s "http://localhost:9200/_cat/shards?v" | grep -E "app|nginx|kafka"'
```

---

## 📊 Полезные команды для мониторинга (обновленные)

```bash
# Проверка индексов с репликами
curl -s "http://localhost:9200/_cat/indices?v"

# Распределение шардов по нодам
curl -s "http://localhost:9200/_cat/shards?v"

# Статистика по нодам
curl -s "http://localhost:9200/_cat/nodes?v"

# Просмотр топиков Kafka
docker exec -it kafka-sandbox /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Consumer groups
docker exec -it kafka-sandbox /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Детали по группе Logstash
docker exec -it kafka-sandbox /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group logstash-group

# Логи Logstash
docker logs -f logstash-sandbox
```

---

## 🎯 Три статуса здоровья кластера (Cluster Health)

| Статус | Значение | Что делать |
|--------|----------|------------|
| 🟢 **Green** | Все primary и replica шарды работают | ✅ Отлично, ничего не надо |
| 🟡 **Yellow** | Все primary работают, но есть проблемы с репликами | ⚠️ Проверить, есть ли свободные ноды |
| 🔴 **Red** | Потеряны primary шарды | 🆘 Срочно восстанавливать из бэкапов |

---

## 🔧 Устранение неполадок (обновленная версия)

### Проблема: Индексы остаются желтыми (yellow) после включения реплик
**Решение:** Проверить, что обе ноды видят друг друга:
```bash
# Проверить ноды в кластере
curl -s "http://localhost:9200/_cat/nodes?v"
# Должно быть 2 ноды

# Если одна нода, проверить логи
docker logs elastic-sandbox-2 --tail 50
```

### Проблема: Logstash пишет в одну ноду
**Решение:** Обновить конфиг с обоими хостами:
```ruby
elasticsearch {
  hosts => ["elasticsearch1:9200", "elasticsearch2:9200"]
  index => "nginx-logs-%{+YYYY.MM.dd}"
}
```

### Проблема: Реплики не распределяются равномерно
**Решение:** Включить балансировку:
```bash
curl -X PUT "http://localhost:9200/_cluster/settings" -H 'Content-Type: application/json' -d'
{
  "transient": {
    "cluster.routing.rebalance.enable": "all"
  }
}'
```

---

## 🏁 Финальная проверка кластера

```bash
# 1. Здоровье кластера (должен быть green)
curl -s "http://localhost:9200/_cluster/health?pretty"

# 2. Количество нод (должно быть 2)
curl -s "http://localhost:9200/_cat/nodes?v" | wc -l

# 3. Распределение шардов (для каждого индекса должны быть p и r)
curl -s "http://localhost:9200/_cat/shards?v" | grep -E "app|nginx|kafka"

# 4. Количество документов
curl -s "http://localhost:9200/_cat/indices?v" | grep -E "app|nginx|kafka"

# 5. Проверка consumer groups (LAG должен быть 0)
docker exec -it kafka-sandbox /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group logstash-group
docker exec -it kafka-sandbox /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group python-consumer-group
```


✅ **2 ноды Elasticsearch** — отказоустойчивый кластер  
✅ **Репликация данных** — каждая запись хранится в двух экземплярах  
✅ **Зеленый статус** — все шарды распределены правильно  
✅ **Балансировка нагрузки** — запросы распределяются между нодами  
✅ **Kafka** — буферизация событий  
✅ **Logstash** — парсинг логов и чтение из Kafka  
✅ **Kibana** — визуализация  
✅ **Python** — генерация и потребление событий  

**Размер кластера:** 2 ноды  
**Репликация:** 1 реплика на каждый индекс  
**Отказоустойчивость:** при падении одной ноды данные не теряются  
**Производительность:** параллельная обработка запросов на двух нодах

