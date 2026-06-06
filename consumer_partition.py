import json
from confluent_kafka import Consumer, TopicPartition, Producer

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ml-scoring-engine',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

producer_config = {'bootstrap.servers': 'localhost:9092'}

consumer = Consumer(consumer_config)
producer = Producer(producer_config)

# Manually assign to Partition 0 instead of using subscribe()
tp = TopicPartition("topic_raw", 0)
consumer.assign([tp])

print("🕵️‍♂️ ML Scoring Engine listening exclusively on 'topic_raw' Partition 0...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        data = json.loads(msg.value().decode('utf-8'))
        print(f"Processing Cairo Transaction: {data['transaction_id']} | Amount: {data['amount']}")

        # Fraud detection rule
        if data['amount'] > 100000:
            print(f"🚨 FRAUD DETECTED: {data['transaction_id']}")
            producer.produce(
                topic="topic_fraud",
                value=json.dumps(data).encode('utf-8')
            )
            producer.flush()
            
except KeyboardInterrupt:
    pass
finally:
    consumer.close()