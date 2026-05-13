# Kafka Streaming Project - Product View

## Description
Real-time data streaming from a remote Kafka server to local Kafka, then consume and process messages.

---

## Architecture
```text
Remote Server (46.202.167.130)
        ↓ real-time stream
   stream_bridge.py
        ↓ produce
Local Kafka (topic: product_view, 3 partitions)
        ↓
consumer1.py + consumer2.py (running simultaneously)
```

---

## Project Structure
```text
LeHoangMy_K21_LV2_Project_01/
├── configs/
│   ├── kafka_config.py          # credentials (not pushed to GitHub)
│   └── kafka_config_example.py  # config template
├── scripts/
│   ├── stream_bridge.py         # stream remote → local
│   ├── consumer1.py             # consumer scenario 1
│   └── consumer2.py             # consumer scenario 2
├── screenshots/                 # result screenshots
└── logs/                        # log files (not pushed to GitHub)
```

---

## Local Kafka Setup

### 1. Check running containers
```bash
docker ps
```

### 2. Create SASL config file for local
```bash
cat > /tmp/kafka-local.properties << 'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="" password="";
EOF

docker cp /tmp/kafka-local.properties kafka-0:/tmp/kafka-local.properties
```

### 3. Create local topic with 3 partitions
```bash
docker exec -it kafka-0 \
  kafka-topics --create \
  --topic product_view \
  --partitions 3 \
  --replication-factor 3 \
  --bootstrap-server kafka-0:29092
```
> Using port 29092 (INTERNAL listener, no SASL required) instead of 9092

### 4. Verify topic created successfully
```bash
docker exec -it kafka-0 \
  kafka-topics --describe \
  --topic product_view \
  --bootstrap-server kafka-0:29092
```

### 5. Read sample data from remote server
```bash
# Create remote config file
cat > /tmp/kafka-client.properties << 'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka" password="UnigapKafka@2024";
EOF

docker cp /tmp/kafka-client.properties kafka-0:/tmp/kafka-client.properties

# Read 10 sample messages
docker exec -it kafka-0 \
  kafka-console-consumer \
  --bootstrap-server 46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294 \
  --topic product_view \
  --from-beginning \
  --max-messages 10 \
  --consumer.config /tmp/kafka-client.properties
```

---

## How to Run

### 1. Install dependencies
```bash
pip3 install confluent-kafka --break-system-packages
```

### 2. Create config file
```bash
cp configs/kafka_config_example.py configs/kafka_config.py
# Fill in your credentials in kafka_config.py
```

### 3. Run stream bridge (terminal 1)
```bash
python3 scripts/stream_bridge.py
```

### 4. Run consumers (terminal 2 + 3)
```bash
python3 scripts/consumer1.py
python3 scripts/consumer2.py
```

### 5. Verify no duplicate messages
```bash
comm -12 <(sort logs/consumer1.log) <(sort logs/consumer2.log)
# Empty output = no duplicates
```

---

## Results and Observations

### Scenario 1: 1 Consumer
- Consumer receives all 3 partitions (P0, P1, P2)
- Processes all messages by itself

### Scenario 2: 2 Consumers in the same group
- Kafka automatically rebalances when consumer 2 joins the group
- Consumer 1 → Partition 2
- Consumer 2 → Partition 0 + 1
- Each message is consumed by exactly 1 consumer (exactly once)

### Comparison with Theory
| | Theory | Actual |
|---|---|---|
| 1 consumer | Receives all partitions | ✅ Correct |
| 2 consumers | Partitions split, no duplicate messages | ✅ Correct |
| Rebalance | Occurs when consumer joins/leaves | ✅ Correct |
| Exactly once | Each message processed only once | ✅ Verified via log comparison |

---

## Screenshots
See `screenshots/` folder for detailed results.