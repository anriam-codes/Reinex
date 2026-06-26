import pandas as pd
import random
import uuid
from datetime import datetime

# Load raw dataset
df = pd.read_parquet("yellow_tripdata_2023-01.parquet")

# Sample for MVP
df = df.sample(n=200, random_state=42).reset_index(drop=True)

events = []

for _, row in df.iterrows():
    event = row.to_dict()

    # Simulated fields
    event["trip_id"] = str(uuid.uuid4())
    event["driver_id"] = random.randint(1, 1000)
    event["rider_id"] = random.randint(1, 5000)
    event["surge_multiplier"] = random.choice([1.0, 1.2, 1.5])

    # Metadata
    event["ingestion_time"] = datetime.utcnow()
    event["source"] = "nyc_taxi"

    events.append(event)

bronze_df = pd.DataFrame(events)

bronze_df.to_parquet(
    "bronze/trips.parquet",
    index=False
)

print(f"Bronze layer created with {len(bronze_df)} records.")