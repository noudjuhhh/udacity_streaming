from confluent_kafka import Consumer, Producer
import time

BROKER_URL = "localhost:9092"
p = Producer({"bootstrap.servers": BROKER_URL, "batch.num.messages": 1, "linger.ms": 1})

iteration = 0
while True:
    p.produce("noet.test.topic", value=f"{iteration}")
    iteration += 1
    time.sleep(1)
    print(p.poll(0))
