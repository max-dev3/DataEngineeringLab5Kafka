import csv, json, time, os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "broker-1:29092")

for attempt in range(30):
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode(),
            acks="all", linger_ms=100
        )
        break
    except NoBrokersAvailable:
        print(f"Спроба {attempt+1}/30")
        time.sleep(2)
else:
    raise RuntimeError("Kafka broker недоступний 60 секунд")

with open("data.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter=";")
    for row in reader:
        msg = {k: (v.strip() if v else "") for k, v in row.items()}
        producer.send("Topic1", value=msg)
        producer.send("Topic2", value=msg)
        print("Sent", msg["trip_id"])
        time.sleep(0.2)

producer.flush()
producer.close()
