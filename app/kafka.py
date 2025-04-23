import uuid
from json import dumps

from kafka import KafkaProducer, KafkaConsumer

# Настройки для подключения к Kafka
bootstrap_servers = 'localhost:9092' # адрес Kafka


def sendKafka(message, key):
    print("sendKafka")
    print(message, key)
    my_producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    my_producer.send(key, value=message)


def getKafka(key):
    print("getKafka")
    consumer = KafkaConsumer(key,
                             bootstrap_servers=bootstrap_servers,
                             group_id=f"test-{uuid.uuid4()}",
                             auto_offset_reset='earliest')

    res_str = []

    message = consumer.poll(timeout_ms=5000)

    for records in message.values():
        for msg in records:
            res_str.append(msg.value.decode("utf-8").strip('"'))

    return res_str

