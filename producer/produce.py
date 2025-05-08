import csv, json, os, time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "broker-1:29092,broker-2:29093")

# Підключення до Kafka
for attempt in range(30):
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            batch_size=65536,
            linger_ms=100
        )
        break
    except NoBrokersAvailable:
        print(f"Спроба {attempt+1}/30: Kafka не готова")
        time.sleep(2)
else:
    raise RuntimeError("Kafka broker недоступний 60 секунд")

# Читання та сортування CSV
with open("data.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter=";")
    rows = [{k: (v.strip() if v else "") for k, v in row.items()} for row in reader]

rows.sort(key=lambda r: datetime.strptime(r["start_time"], "%d.%m.%Y %H:%M"))

# відправка
for msg in rows:
    producer.send("Topic1", value=msg)
    producer.send("Topic2", value=msg)

producer.flush()
producer.close()
