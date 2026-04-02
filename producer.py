import pandas as pd
import random
from datetime import datetime

# load dataset
df = pd.read_parquet("yellow_tripdata_2023-01.parquet")

# sample for MVP
df = df.sample(n=200, random_state=42).reset_index(drop=True)

events = []

for i, row in df.iterrows():
    event = row.to_dict()

    # Simulated Fields
    event["trip_id"] = f"trip_{i}"
    event["driver_id"] = random.randint(1, 1000)
    event["rider_id"] = random.randint(1, 5000)
    event["surge_multiplier"] = random.choice([1.0, 1.2, 1.5])

    # Metadata
    event["ingestion_time"] = datetime.utcnow()
    event["source"] = "nyc_taxi"

    events.append(event)

bronze_df = pd.DataFrame(events)
bronze_df.to_parquet("bronze/trips.parquet", index=False)
print("Bronze layer created with", len(bronze_df), "records")