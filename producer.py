import sys
import uuid
import random
import pandas as pd
from datetime import datetime, UTC

# Check command-line argument
if len(sys.argv) != 2:
    print("Usage: python producer.py <dataset_path>")
    sys.exit(1)

dataset_path = sys.argv[1]

# Load raw dataset
df = pd.read_parquet(dataset_path)

# Sample for MVP
df = df.sample(n=200, random_state=42).reset_index(drop=True)

events = []

for _, row in df.iterrows():
    event = row.to_dict()

    # Simulated Fields
    event["trip_id"] = str(uuid.uuid4())
    event["driver_id"] = random.randint(1, 1000)
    event["rider_id"] = random.randint(1, 5000)
    event["surge_multiplier"] = random.choice([1.0, 1.2, 1.5])

    # CDC Simulate
    operation = random.choices(
        ["INSERT", "UPDATE", "DELETE"],
        weights=[90, 8, 2],
        k=1
    )[0]

    event["operation"] = operation
    event["updated_at"] = datetime.now(UTC)

    # Simulate business updates
    if operation == "UPDATE":
        event["fare_amount"] = round(event["fare_amount"] * 1.10, 2)
        event["tip_amount"] = round(event["tip_amount"] + 2.00, 2)
        event["total_amount"] = round(event["total_amount"] + 2.00, 2)

    # Metadata
    event["ingestion_time"] = datetime.now(UTC)
    event["source"] = "nyc_taxi"

    events.append(event)

# Bronze Layer
bronze_df = pd.DataFrame(events)

bronze_df.to_parquet(
    "bronze/trips.parquet",
    index=False
)

print(f"Bronze layer created with {len(bronze_df)} records.")
print(bronze_df["operation"].value_counts())