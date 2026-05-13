from confluent_kafka import Consumer, Producer
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from configs.kafka_config import REMOTE_CONFIG, LOCAL_CONFIG, TOPIC


def delivery_report(err, msg):
    if err is not None:
        print(f"Produce thất bại: {err}")
    else:
        print(f"✓ Produce thành công → partition {msg.partition()} offset {msg.offset()}")


def main():
    consumer = Consumer(REMOTE_CONFIG)
    producer = Producer(LOCAL_CONFIG)
    consumer.subscribe([TOPIC])

    print(f"Stream bridge đang chạy... topic: {TOPIC}")
    print("Ctrl+C để dừng\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Lỗi consumer: {msg.error()}")
                continue

            # Đẩy sang Kafka local
            producer.produce(
                topic=TOPIC,
                value=msg.value(),
                key=msg.key(),
                callback=delivery_report
            )
            producer.flush()

            # Commit thủ công sau khi produce xong → exactly once
            consumer.commit(msg)

    except KeyboardInterrupt:
        print("\nDừng stream bridge.")
    finally:
        consumer.close()
        print("Đã đóng kết nối.")


if __name__ == '__main__':
    main()