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

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "broker-1:29092")
TOPIC     = "Topic1"
GROUP     = "consumer-group-1"
CSV_DIR   = "./csvs"
BUCKET    = "default"

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


# Створюємо чергу для завантаження файлів у MinIO
upload_queue = queue.Queue()

def upload_worker():
    while True:
        fname = upload_queue.get()  # Беремо ім’я файлу з черги
        if fname is None:
            break

        local_path = os.path.join(CSV_DIR, fname)  # Формуємо повний шлях до файлу

        if os.path.exists(local_path):  # Перевіряємо, чи існує файл на диску
            # Завантажуємо файл у MinIO в бакет
            minio_client.fput_object(BUCKET, fname, local_path)

            # Після успішного завантаження  видаляємо локальний файл
            os.remove(local_path)

            print(f"[MinIO] Uploaded & removed {fname}")

        upload_queue.task_done()  # Повідомляємо, що завдання з черги виконано

uploader_thread = threading.Thread(target=upload_worker, daemon=True)
uploader_thread.start()

# Основний цикл:
buffers = {}          # Словник для зберігання записів, згрупованих по місяцях
current_month = None  # Поточний ключ (місяць_рік), за яким зараз групуються повідомлення

try:
    for msg in consumer:  # цикл обробки повідомлень з Kafka
        rec = msg.value   # Отримуємо повідомлення (словник з CSV-рядка)

        # розпарсимо дату початку з поля start_time
        try:
            dt = datetime.strptime(rec["start_time"], "%d.%m.%Y %H:%M")
        except Exception:
            continue  # Якщо дата не валідна — пропускаємо повідомлення

        # Створюємо ключ у форматі MM_YYYY
        key = f"{dt.month:02d}_{dt.year}"

        # Якщо місяць змінився (тобто почали надходити записи іншого місяця)
        if current_month and key != current_month:
            # Формуємо назву файлу для попереднього місяця
            out_fname = f"{current_month}.csv"

            # Записуємо всі накопичені записи в CSV-файл
            pd.DataFrame(buffers[current_month]) \
              .to_csv(os.path.join(CSV_DIR, out_fname), index=False)

            # Додаємо ім'я файлу в чергу на завантаження до MinIO
            upload_queue.put(out_fname)

            # Видаляємо буфер попереднього місяця з пам’яті
            del buffers[current_month]

        # Додаємо запис у буфер для відповідного місяця
        buffers.setdefault(key, []).append(rec)
        current_month = key  # Оновлюємо поточний місяць


        print(f"[{GROUP}] {msg.topic}:{msg.offset} - trip_id={rec.get('trip_id')} - {key}")

except KeyboardInterrupt:
    print("\nInterrupted by user")

finally:
    # Якщо ще залишились дані за останній оброблений місяць — зберігаємо їх
    if current_month and current_month in buffers:
        # Формуємо назву фінального CSV-файлу
        final_fname = f"{current_month}.csv"
        # Перетворюємо буфер (список словників) у DataFrame і зберігаємо як CSV
        pd.DataFrame(buffers[current_month]) \
          .to_csv(os.path.join(CSV_DIR, final_fname), index=False)
        # Додаємо цей файл у чергу на завантаження в MinIO
        upload_queue.put(final_fname)

    # Чекаємо, поки всі файли завантажаться в MinIO
    upload_queue.join()

    # Завершуємо роботу фонового потоку завантаження
    upload_queue.put(None)
    uploader_thread.join()

    # Закриваємо Kafka consumer
    consumer.close()
