from confluent_kafka import Consumer, KafkaError

topic_name = 'example-device-data'
bootstrap_servers = ['ip_address:9092']
group_id = 'my-consumer-group'

conf = {
    'bootstrap.servers': ','.join(bootstrap_servers),
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([topic_name])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f'Consume error: {msg.error()}')
                break
        
        key = msg.key().decode("utf-8") if msg.key() else None
        value = msg.value().decode("utf-8") if msg.value() else None
        
        print(value)

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
