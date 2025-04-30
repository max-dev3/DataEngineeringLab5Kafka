import json, time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

BOOT = "broker-1:29092"
TOPIC = "Topic2"
GROUP = "consumer-group-2"

for i in range(30):
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOT,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=GROUP
        )
        break
    except NoBrokersAvailable:
        print(f"[{TOPIC}] Kafka не готова ({i+1}/30)…")
        time.sleep(2)
else:
    raise RuntimeError("Kafka broker недоступний 60 с")

for msg in consumer:
    print(f"[{TOPIC}] Received:", msg.value)
