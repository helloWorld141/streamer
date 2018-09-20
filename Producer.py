from confluent_kafka import Producer

p = Producer({'bootstrap.servers': '43.240.97.180:9092'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

count = 0

while True :
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)
    count += 1
    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    try:
        p.produce('stream-sim', ("message " + str(count)).encode('utf-8'), callback=delivery_report)
    except BufferError:
        continue
"""
p.poll(0)
p.produce('stream-sim', ("message " + str(count)).encode('utf-8'), callback=delivery_report)
"""

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
