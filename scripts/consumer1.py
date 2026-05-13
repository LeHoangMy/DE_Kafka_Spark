from confluent_kafka import Consumer
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from configs.kafka_config import LOCAL_CONFIG, TOPIC

# Override config cho consumer
consumer_config = {
    **LOCAL_CONFIG,
    'group.id': 'local-consumer-group',
    'auto.offset.reset': 'earliest',
}

def main():
    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC])

    print(f"Consumer 1 đang chạy... topic: {TOPIC}")
    print("Ctrl+C để dừng\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Lỗi: {msg.error()}")
                continue

            print(f"[C1] Partition {msg.partition()} | Offset {msg.offset()} | {msg.value().decode('utf-8')[:80]}...")

    except KeyboardInterrupt:
        print("\nDừng consumer 1.")
    finally:
        consumer.close()
        print("Đã đóng kết nối.")

if __name__ == '__main__':
    main()