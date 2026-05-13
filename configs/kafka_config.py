# Remote Kafka Server
REMOTE_CONFIG = {
    'bootstrap.servers': '46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024',
    'group.id': 'bridge-consumer-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False,
}

# Local Kafka
LOCAL_CONFIG = {
    'bootstrap.servers': 'localhost:9094',
}

TOPIC = 'product_view'