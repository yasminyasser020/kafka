# Kafka Lab
# 🛠️ Native Python Kafka Pipelines & Local Data Governance

An end-to-end event-driven architecture using native Python clients to manage manual partition routing, local Avro data contracts, direct Parquet storage sinking, and serverless Lakehouse analytics via DuckDB.

---

## 🏗️ Pipeline Architecture

```text
       [ Advanced Producer ]                 [ Local Avro Producer ]
                 │                                      │
        (Location Routing)                      (Local Contract)
                 │                                      │
                 ▼                                      ▼
     topic_raw (2 Partitions)                sales_topic (Binary)
        │                 │                             │
    [Cairo: P0]     [Alexandria: P1]                    ▼
        │                 │                   [ Local Avro Consumer ]
        ▼                 ▼                   (Schemaless Deserializer)
 [Isolated Consumer]   (Dropped)
 (Filters > 100k)
        │
        ▼
   topic_fraud
        │
        ▼
  [ Parquet Sink ]
        │
        ▼
 data_lake/*.parquet ──► [ DuckDB Analytical Engine ]
 ```
 ---

## 📋 Unified Data Schema Definitions

### 1. JSON Structural Flow (`topic_raw` / `topic_fraud`)
Standard plain-text serialization utilizing an implicit schema. This layout requires defensive programming blocks at the consumer layer.

| Field Name | Type | Description |
| :--- | :--- | :--- |
| `transaction_id` | `string` | Unique transaction identifier |
| `user_id` | `integer` | Unique identification number for the user |
| `amount` | `float` | Transaction value currency unit |
| `location` | `string` | Geographical transaction source city |

### 2. Avro Local Data Contract (`sales_topic`)
Strict client-side enforced binary data definition utilizing `fastavro`.

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "iti.ai.track.local",
  "fields": [
    {"name": "order_id", "type": "int"},
    {"name": "item_name", "type": "string"},
    {"name": "price", "type": "float"}
  ]
}
```

---

## 🏗️ Environment Setup

### Step 1: Launch Infrastructure Cluster
Spin up the local containerized environment containing your multi-broker Kafka architecture, and object storage layers:
```bash
docker compose up -d 
```

### Step 2: Install Native Dependencies
Install the native Python client libraries needed to interface with the cluster and handle file structures:
```bash
pip install confluent-kafka[avro] fastavro pandas pyarrow duckdb
```

### Step 3: Initialize Topic Topology
Execute these commands inside your workspace terminal to prepare your message streams with dedicated partition properties:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic topic_raw --partitions 2 --replication-factor 3
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic topic_fraud --partitions 1 --replication-factor 3
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sales_topic --partitions 2 --replication-factor 3
```

## 🧪 Operational Step Guide & Component Logic

### 🔍 Task 1: Real-Time Fraud Pipeline, Parquet Sinking & DuckDB Analytics

* **`producer_advanced.py`**: Acts as the data generator. Dispatches transaction data to explicit partitions based on a structural business rule:
  * `location == "Cairo"` $\rightarrow$ **Partition 0**
  * `location == "Alexandria"` $\rightarrow$ **Partition 1**
* **`consumer_partition.py`**: Mimics an ML scoring engine. Instead of subscribing to all partitions, it isolates processing bandwidth by manually pinning execution strictly to **Partition 0** only to consume Cairo transactions. If `amount > 100000`, it forwards the anomalous record to `topic_fraud`.
* **`test_offsets.py`**: Evaluates consumer group behaviors. Combines unique timestamped `group.id` parameters with execution modifiers:
  * `latest`: Ignores stream logs history, reading live entries exclusively.
  * `earliest`: Rewinds reading bookmark position to **Offset 0**, parsing all historical records sequentially.
* **`consumer_to_parquet.py`**: Pulls logs from `topic_fraud`, accumulates alerts, and periodically writes them as structured Parquet files directly to the `data_lake/` directory using explicit `PyArrow` schemas.
* **`analytics.py`**: Queries all Parquet files directly using serverless **DuckDB** integration to execute advanced relational SQL analytical operations over column-oriented files directly on disk without needing Spark or an external database server.

---

### 🛡️ Task 2: Client-Side Avro Data Governance

* **`producer_avro_local.py`**: Introduces client-side data governance using local Python parsing libraries instead of an external server container. Enforces strict payload governance locally using `fastavro.validation.validate()`.
  * **Test Case A (The Valid Run)**: Submits cleanly typed inputs matching the layout types precisely to verify flawless binary delivery.
  * **Test Case B (The Broken Schema Run)**: Attempts to pass an explicit string value into the integer `order_id` field, forcing a local runtime validation crash to intercept data transmission before invalid bytes reach the broker network wire.
* **`consumer_avro_local.py`**: Subscribes to the target partition array, extracts the raw binary payload array, wraps it inside an in-memory stream, and applies `fastavro.schemaless_reader` using a localized file layout configuration copy to decode the data back into native Python dictionaries.

## 🏁 Lab Execution Run-Order Checklist

Open four distinct workspace terminal windows and execute the files sequentially to run the processing environment:

### Step A: Initialize the Analytics Pipeline Layer

1. **Terminal 1** (Lakehouse Sink Engine):
   ```bash
   python consumer_to_parquet.py
   ```


2. **Terminal 2** (ML Scoring Filter Processor):

   ```bash
    python consumer_partition.py
    ```

3. **Terminal 3** (Data Generation Producer):

    ```bash
    python producer_advanced.py
    ```

### Step B: Run Parquet Tabular Sweeps
Once micro-batches confirm file commitments to the directory structure:

4. **Terminal 4** (DuckDB Query Execution Engine):

    ```bash
    python analytics.py
    ```
    
### Step C: Evaluate Local Governance Contracts
Stop previous streams and initiate tests across local parsing boundaries:

1. **Terminal 1** (Local Avro Reader Engine):

    ```bash
    python consumer_avro_local.py
    ```
2. **Terminal 2** (Execute Passing Case):

    ```bash
    python producer_avro_local.py A
    ```
3. **Terminal 2** (Execute Failing Case Safeguard Verification):

    ```bash
    python producer_avro_local.py B
    ```