from confluent_kafka import Consumer, KafkaError
import time

c = Consumer({
    'bootstrap.servers': 'kafka-master:9092',
    'group.id': 'mygroup',
    'default.topic.config': {
        'auto.offset.reset': 'smallest'
    }
})

c.subscribe(['stream-sim'])
with open("out", "w") as f:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        values = msg.value().decode('utf-8')
        print('Received message: {}'.format(values))
        f.seek(0,0) # return to the beginning of file
        f.write(values)
        f.truncate() # remove every thing after the last write
        time.sleep(1)

c.close()
