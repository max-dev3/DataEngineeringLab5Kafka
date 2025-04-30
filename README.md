# Lab 5 — Kafka Producer & Consumers with Docker

### How It Works

1. The producer waits until Kafka brokers are available.
2. It parses the CSV file, strips spaces, converts each row to JSON.
3. Each message is sent to both topics.
4. Consumers wait few seconds (for broker availability) and then consume from their topics.
## Run commands

```bash
# Build and start all services
docker compose up --build -d

# View logs
docker compose logs -f producer
docker compose logs -f consumer1
docker compose logs -f consumer2
docker compose logs -f kafka-ui

# Stop all
docker compose down
