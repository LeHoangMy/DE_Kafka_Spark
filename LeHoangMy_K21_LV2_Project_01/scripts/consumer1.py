from confluent_kafka import Consumer
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from configs.kafka_config import LOCAL_CONFIG, TOPIC

consumer_config = {
    **LOCAL_CONFIG,
    'group.id': 'local-consumer-group',
    'auto.offset.reset': 'earliest',
}

def main():
    os.makedirs('logs', exist_ok=True)
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

            log_line = f"{msg.partition()},{msg.offset()}"
            print(f"[C1] Partition {msg.partition()} | Offset {msg.offset()}")

            with open('logs/consumer1.log', 'a') as f:
                f.write(log_line + '\n')

    except KeyboardInterrupt:
        print("\nDừng consumer 1.")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()