from confluent_kafka import Consumer, TopicPartition

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'
})

# c.subscribe(['my-topic'])
c.assign([TopicPartition('my-topic', 0)])
# print(c.list_topics().cluster_id)
# print(c.list_topics("my-topic").topics)
# print(c.list_topics().brokers)
"""
The Consumer has a few APIs to control its state:

pause()/resume(): Allows to stop/resume consuming from a set of partitions. The Consumer stays subscribed (so no 
rebalance) but just does not fetch any new records until resumed 

unsubscribe(): Allows to change consumer subscription, if not subscribed to anything, it will just stay connected to 
the cluster. 

seek(): consume data from a topic partition at a particular offset, usually offsets for times method would be called
first and then seek method is called.
list_topics(): ClusterMetadata object is returned
If you are "done" with the Consumer, you can also call close() and start a new one when needed
"""

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()