import pandas as pd
import random

df = pd.read_parquet("yellow_tripdata_2023-01.parquet")

df = df.sample(n=200, random_state=42)

events = []

for _, row in df.iterrows():
    event = row.to_dict()

    event["driver_id"] = random.randint(1, 1000)
    event["rider_id"] = random.randint(1, 5000)
    event["surge_multiplier"] = random.choice([1.0, 1.2, 1.5])

    events.append(event)

# convert to dataframe
bronze_df = pd.DataFrame(events)

# save to bronze layer
bronze_df.to_parquet("bronze/trips.parquet", index=False)

print("Bronze layer created with", len(bronze_df), "records")