import json
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'rc1a-nagt18he23cqhb4i.mdb.yandexcloud.net:9091,rc1b-srnvnsjpk02n269k.mdb.yandexcloud.net:9091,rc1d-4jkv4e64litah8vg.mdb.yandexcloud.net:9091',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'mkf-user',
    'sasl.password': '12345678',
}

producer = Producer(conf)

with open('sample.json', 'r') as f:
    messages = json.load(f)

for msg in messages:
    producer.produce(
        topic='sensors',
        key=msg['device_id'],
        value=json.dumps(msg)
    )
    print(f"Отправлено: {msg['device_id']}")

producer.flush()
print("Все сообщения отправлены!")