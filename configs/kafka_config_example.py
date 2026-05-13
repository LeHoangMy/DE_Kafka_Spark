# Remote Kafka Server
REMOTE_CONFIG = {
    'bootstrap.servers': 'YOUR_BOOTSTRAP_SERVERS',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'YOUR_USERNAME',
    'sasl.password': 'YOUR_PASSWORD',
    'group.id': 'bridge-consumer-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False,
}

# Local Kafka
LOCAL_CONFIG = {
    'bootstrap.servers': 'localhost:9094',
}

TOPIC = 'product_view'