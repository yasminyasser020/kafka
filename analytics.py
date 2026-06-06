import duckdb
import os

print("🦆 Initializing Deep-Dive Analytical Suite on Active Parquet Pool...\n")

parquet_path = "data_lake/*.parquet"

if not os.path.exists("data_lake") or len(os.listdir("data_lake")) == 0:
    print("❌ No Parquet files found in 'data_lake/'. Please run your components first.")
else:
    # 1. Detailed Audit Log (Unpacking the entire file)
    print("📋 [VIEW 1] Detailed Fraud Incidents Audit Log:")
    duckdb.sql(f"""
        SELECT 
            transaction_id,
            user_id,
            location,
            amount,
            -- Add a flag to show how far above the fraud threshold ($100k) this is
            (amount - 100000) AS exposure_above_threshold
        FROM read_parquet('{parquet_path}')
        ORDER BY amount DESC;
    """).show()

    print("-" * 60)

    # 2. Executive Aggregates Matrix
    print("📊 [VIEW 2] Regional Risk & Financial Exposure Summary:")
    duckdb.sql(f"""
        SELECT 
            location, 
            COUNT(*) AS total_fraud_incidents, 
            FORMAT('${{}}', CAST(SUM(amount) AS BIGINT)) AS total_stolen_exposure,
            FORMAT('${{}}', CAST(AVG(amount) AS BIGINT)) AS average_fraud_ticket,
            FORMAT('${{}}', CAST(MAX(amount) AS BIGINT)) AS worst_case_incident
        FROM read_parquet('{parquet_path}')
        GROUP BY location;
    """).show()