
from confluent_kafka import Producer

jsonString1 = """ {"name":"sai", "email":"sai@gmail.com", "salary": "developer"} """
jsonString2 = """ {"name":"ram", "email":"ram@gmail.com", "salary": "tester"} """
jsonString3 = """ {"name":"varun", "email":"varun@gmail.com", "role": "manager"} """

jsonv1 = jsonString1.encode()
jsonv2 = jsonString2.encode()
jsonv3 = jsonString3.encode()


def delivery_callback(errmsg, msg):
    """
    Reports the Failure or Success of a message delivery.
    Args:
        errmsg (KafkaError): The Error that occurred while message producing.
        msg (Actual message): The message that was produced.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


kafka_topic_name = "new-topic"


print("Starting Kafka Producer")
conf = {
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,
    'acks': "all"
}

print("connecting to Kafka topic...")
producer1 = Producer(conf)

# Trigger any available delivery report callbacks from previous produce() calls
producer1.poll(0)

try:
    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    producer1.produce(topic=kafka_topic_name, key="dev", value=jsonv1, on_delivery=delivery_callback)
    producer1.produce(topic=kafka_topic_name, key="tester", value=jsonv2, on_delivery=delivery_callback)
    producer1.produce(topic=kafka_topic_name, key="manager", value=jsonv3, on_delivery=delivery_callback)


except Exception as ex:
    print("Exception happened :", ex)
print("Wait until all messages have been delivered")
producer1.flush()
print("\n Stopping Kafka Producer")
