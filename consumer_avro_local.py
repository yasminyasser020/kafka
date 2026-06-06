import io
from confluent_kafka import Consumer
from fastavro import parse_schema, schemaless_reader

# Load the exact same local structural layout used by the producer
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
parsed_schema = parse_schema(avro_schema_definition)

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'local-avro-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(consumer_config)
consumer.subscribe(['sales_topic'])

print("📥 Local Avro Consumer Active. Listening for raw binary payloads on 'sales_topic'...\n")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer Error: {msg.error()}")
            continue

        # Extract the raw byte array from the message body
        raw_bytes = msg.value()
        
        # Wrap the raw byte package into an in-memory byte stream container
        bytes_io = io.BytesIO(raw_bytes)
        
        # Deserialization Process: Decode using our local copy blueprint
        decoded_record = schemaless_reader(bytes_io, parsed_schema)
        
        print("🎯 Successfully Decoded Binary Bytes from Cluster:")
        print(f"   Order ID:  {decoded_record['order_id']} (Type: {type(decoded_record['order_id']).__name__})")
        print(f"   Item Name: {decoded_record['item_name']}")
        print(f"   Price:     ${decoded_record['price']:.2f}")
        print("-" * 50)

except KeyboardInterrupt:
    print("\n👋 Consumer stopped.")
finally:
    consumer.close()