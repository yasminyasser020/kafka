import os
import json
from confluent_kafka import Consumer
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'parquet-lakehouse-sink',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(config)
consumer.subscribe(['topic_fraud'])

OUTPUT_DIR = "data_lake"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Schema strict alignment definition
arrow_schema = pa.schema([
    ('transaction_id', pa.string()),
    ('user_id', pa.int64()),
    ('amount', pa.float64()),
    ('location', pa.string())
])

print("📥 Parquet Lakehouse Sink initialized. Monitoring 'topic_fraud'...")

batch = []
BATCH_SIZE = 2  # Flushes file to lake house directory per 2 events

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            continue

        record = json.loads(msg.value().decode('utf-8'))
        batch.append(record)
        print(f"Staged fraud alert: {record['transaction_id']}")

        if len(batch) >= BATCH_SIZE:
            df = pd.DataFrame(batch)
            
            # Explicit casting to preserve type definitions
            df['user_id'] = df['user_id'].astype('int64')
            df['amount'] = df['amount'].astype('float64')
            
            table = pa.Table.from_pandas(df, schema=arrow_schema)
            
            file_name = f"{OUTPUT_DIR}/fraud_{int(df['user_id'].iloc[0])}_{df['transaction_id'].iloc[0]}.parquet"
            pq.write_table(table, file_name)
            print(f"💾 Committed batch safely to Lakehouse storage path: {file_name}")
            
            batch.clear()  # Purge explicit RAM batch state

except KeyboardInterrupt:
    pass
finally:
    consumer.close()