from confluent_kafka import Consumer, Producer
import time

BROKER_URL = "localhost:9092"
c = Consumer(
    {
        "bootstrap.servers": BROKER_URL,
        "group.id": "tester",
        "auto.offset.reset": "earliest",
    }
)
c.subscribe(["noet.test.topic"])

iteration = 0
while True:
    iteration += 1
    msg = c.poll(10)
    if msg is None:
        print("no message")
    else:
        print(msg.value())
