import sys
import time
from confluent_kafka import Consumer

if len(sys.argv) < 2 or sys.argv[1] not in ['earliest', 'latest']:
    print("Usage: python test_offsets.py [earliest|latest]")
    sys.exit(1)

strategy = sys.argv[1]

# 🕒 Dynamic Timestamping: Generates a completely unique group string on every run
unique_group_id = f"offset-tester-group-{strategy}-{int(time.time())}"

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': unique_group_id,       #  Forced fresh group
    'auto.offset.reset': strategy,      #  Your strategy argument
    'enable.auto.commit': False
}

consumer = Consumer(config)
consumer.subscribe(['topic_raw'])

print(f"📊 Strategy Test Booted: {strategy.upper()}")
print(f"🆔 Consumer Group ID: {unique_group_id}")
print("📡 Listening... (Press Ctrl+C to manually stop)")
print("-" * 60)

try:
    # Infinite loop: This ensures the script NEVER terminates on its own
    while True:
        msg = consumer.poll(1.0) # Check for data every 1 second
        
        if msg is None:
            # Keeps the terminal alive and waiting patiently when there is no data
            continue
            
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        # Print the data when it arrives
        print(f"📥 [Partition {msg.partition()}] Received: {msg.value().decode('utf-8')}")
        
except KeyboardInterrupt:
    print("\n👋 Experiment stopped manually by user.")
finally:
    consumer.close()