import io
import sys
from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer
from fastavro.validation import validate, ValidationError

# 1. Hardcoded Strict Blueprint matching your specs
avro_schema_definition = {
    "type": "record",
    "name": "Order",
    "namespace": "iti.ai.track.local",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "item_name", "type": "string"},
        {"name": "price", "type": "float"}
    ]
}

# Parse the schema into a fastavro-optimized format
parsed_schema = parse_schema(avro_schema_definition)

# Configure the Kafka Producer targeting our KRaft cluster
producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)
topic_name = "sales_topic"

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"🛡️ Local Avro message delivered to {msg.topic()} [Partition {msg.partition()}]")

# =====================================================================
# EXPERIMENT SETUP
# =====================================================================
if len(sys.argv) < 2 or sys.argv[1] not in ['A', 'B']:
    print("Usage: python producer_avro_local.py [A|B]")
    print("  A: Test Case A (Valid Run)")
    print("  B: Test Case B (Broken Schema Run)")
    sys.exit(1)

test_case = sys.argv[1]

if test_case == 'A':
    # Test Case A: Perfectly aligned types
    data_payload = {"order_id": 101, "item_name": "High-Performance Laptop", "price": 1200.50}
    print("🧪 Running Test Case A: Valid aligned data structure...")
else:
    # Test Case B: Broken Schema (string injected where an int belongs)
    data_payload = {"order_id": "ABC_NOT_AN_INT", "item_name": "Malicious Payload", "price": 99.99}
    print("💥 Running Test Case B: Corrupting order_id field type...")

# =====================================================================
# THE ENFORCEMENT & SERIALIZATION STEP
# =====================================================================
try:
    # Step A: Local client-side validation check
    validate(data_payload, parsed_schema)
    print("✅ Local Client-Side Validation Passed! Encoding record...")

    # Step B: Memory buffer stream serialization
    bytes_io = io.BytesIO()
    schemaless_writer(bytes_io, parsed_schema, data_payload)
    raw_binary_payload = bytes_io.getvalue() # Extract optimized binary array

    # Step C: Ship the raw bytes to Kafka
    producer.produce(topic=topic_name, value=raw_binary_payload, callback=delivery_report)
    producer.flush()

except ValidationError as e:
    print("\n🚨 DATA GOVERNANCE CONTRACT VIOLATION DETECTED! 🚨")
    print(f"The local fastavro validator blocked transmission to the network wire.")
    print(f"Error Details: {e}")
    sys.exit(1)