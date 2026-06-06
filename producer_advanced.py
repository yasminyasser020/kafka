import json
import time
from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'advanced-producer'
}

producer = Producer(config)
topic_name = "topic_raw"

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [Partition {msg.partition()}]")

# Sample data mimicking real-time streaming
transactions = [
    #{"transaction_id": "TX101", "user_id": 1001, "amount": 1500.0, "location": "Cairo"},
    #{"transaction_id": "TX102", "user_id": 1002, "amount": 105000.0, "location": "Cairo"}, # Fraud alert trigger
    #{"transaction_id": "TX103", "user_id": 1003, "amount": 500.0, "location": "Alexandria"},
    #{"transaction_id": "TX104", "user_id": 1004, "amount": 120000.0, "location": "Alexandria"}, # Outside Partition 0
    #{"transaction_id": "TX105", "user_id": 1005, "amount": 250000.0, "location": "Cairo"}  # Fraud alert trigger
     {"transaction_id": "TX106", "user_id": 1006, "amount": 266100.0, "location": "Cairo"} 
    #{
     #   "transaction_id": "TX_SABOTAGE_1", 
      #  "user_id": "NOT_AN_INT_HELP",      # ❌ Should be an int
       # "amount": "ONE_MILLION_DOLLARS",   # ❌ Should be a float
        #"location": "Cairo"
    #}
]

print("🚀 Starting Advanced Producer...")
for tx in transactions:
    # Business Routing Rule
    if tx["location"] == "Cairo":
        target_partition = 0
    elif tx["location"] == "Alexandria":
        target_partition = 1
    else:
        continue # Skip other locations for this lab

    payload = json.dumps(tx).encode('utf-8')
    
    # Produce strictly to the designated partition
    producer.produce(
        topic=topic_name, 
        value=payload, 
        partition=target_partition, 
        callback=delivery_report
    )
    producer.poll(0)
    time.sleep(1)

producer.flush()
print("🏁 All messages dispatched.")