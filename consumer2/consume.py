import os
import json
import time
import threading
import queue
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from minio import Minio
import pandas as pd

BOOTSTRAP = "broker-2:29093"
TOPIC = "Topic2"
GROUP = "consumer-group-2"
CSV_DIR = "./csvs"
BUCKET = "default"


# Підключаємося до Kafka
for attempt in range(30):
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=GROUP
        )
        break
    except NoBrokersAvailable:
        print(f"[{TOPIC}] Kafka not ready ({attempt+1}/30)…")
        time.sleep(2)
else:
    raise RuntimeError("Kafka broker unavailable after 60s")

# Підключаємося до MinIO
minio_client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ROOT_USER", "admin"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD", "adminadmin!!"),
    secure=False
)

# ——————————————————————————————————————————————————————————————————————————
# Підготуємо фоновий воркер для завантаження
upload_queue = queue.Queue()

def upload_worker():
    while True:
        fname = upload_queue.get()
        if fname is None:
            break
        local_path = os.path.join(CSV_DIR, fname)
        if os.path.exists(local_path):
            minio_client.fput_object(BUCKET, fname, local_path)
            os.remove(local_path)
            print(f"[MinIO] Uploaded & removed {fname}")
        upload_queue.task_done()

uploader_thread = threading.Thread(target=upload_worker, daemon=True)
uploader_thread.start()

# Основний цикл: накопичуємо записи по місяцях
buffers = {}
current_month = None

try:
    for msg in consumer:
        rec = msg.value
        # розпарсимо дату
        try:
            dt = datetime.strptime(rec["start_time"], "%d.%m.%Y %H:%M")
        except Exception:
            continue

        key = f"{dt.month:02d}_{dt.year}"

        # якщо місяць змінився — дампимо попередній у файл + в чергу на upload
        if current_month and key != current_month:
            out_fname = f"{current_month}.csv"
            pd.DataFrame(buffers[current_month]) \
              .to_csv(os.path.join(CSV_DIR, out_fname), index=False)
            upload_queue.put(out_fname)
            del buffers[current_month]

        buffers.setdefault(key, []).append(rec)
        current_month = key

        print(f"[{GROUP}] {msg.topic}:{msg.offset} - trip_id={rec.get('trip_id')} - {key}")

except KeyboardInterrupt:
    print("\nInterrupted by user")

finally:
    # Зливаємо останній місяць
    if current_month and current_month in buffers:
        final_fname = f"{current_month}.csv"
        pd.DataFrame(buffers[current_month]) \
          .to_csv(os.path.join(CSV_DIR, final_fname), index=False)
        upload_queue.put(final_fname)
        print(f"[{GROUP}] FINAL FLUSH month={current_month}")

    # Чекаємо, поки всі завантажаться, й дамо знати воркеру, щоб вийшов
    upload_queue.join()
    upload_queue.put(None)
    uploader_thread.join()
    consumer.close()
