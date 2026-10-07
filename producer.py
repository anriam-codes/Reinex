import sys
import random
import pandas as pd
import hashlib
from datetime import datetime, UTC
from pathlib import Path


if len(sys.argv) != 2:
    print("Usage: python producer.py <dataset_path>")
    sys.exit(1)

dataset_path = sys.argv[1]

SOURCE_STATE = Path("bronze/source_state.parquet")
CDC_OUTPUT = Path("bronze/trips.parquet")

SOURCE_STATE.parent.mkdir(exist_ok=True)


# -----------------------
# Load source dataset
# -----------------------

df = pd.read_parquet(dataset_path)

df = df.sample(
    n=200,
    random_state=42
).reset_index(drop=True)


# -----------------------
# Stable Trip ID
# -----------------------

def create_trip_id(row):

    source_key = (
        f"{row['VendorID']}_"
        f"{row['tpep_pickup_datetime']}_"
        f"{row['tpep_dropoff_datetime']}_"
        f"{row['PULocationID']}_"
        f"{row['DOLocationID']}"
    )

    return hashlib.sha256(
        source_key.encode()
    ).hexdigest()


df["trip_id"] = df.apply(
    create_trip_id,
    axis=1
)


# -----------------------
# Source Data Validation
# -----------------------

df["tpep_pickup_datetime"] = pd.to_datetime(
    df["tpep_pickup_datetime"]
)

df["tpep_dropoff_datetime"] = pd.to_datetime(
    df["tpep_dropoff_datetime"]
)

df = df[
    (df["fare_amount"] > 0)
    & (df["trip_distance"] > 0)
]

df = df.dropna(
    subset=[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "fare_amount",
        "trip_distance",
    ]
).reset_index(drop=True)


# -----------------------
# Create Initial State
# -----------------------

def create_initial_state():

    events = []

    for _, row in df.iterrows():

        event = row.to_dict()

        event["driver_id"] = random.randint(1, 1000)
        event["rider_id"] = random.randint(1, 5000)
        event["surge_multiplier"] = random.choice(
            [1.0, 1.2, 1.5]
        )

        event["operation"] = "INSERT"
        event["updated_at"] = datetime.now(UTC)
        event["ingestion_time"] = datetime.now(UTC)
        event["source"] = "nyc_taxi"

        events.append(event)

    return pd.DataFrame(events)


# -----------------------
# FIRST RUN
# -----------------------

if not SOURCE_STATE.exists():

    source_state = create_initial_state()

    source_state.to_parquet(
        SOURCE_STATE,
        index=False
    )

    source_state.to_parquet(
        CDC_OUTPUT,
        index=False
    )

    print("Initial source state created.")
    print(
        f"Valid source records: "
        f"{len(source_state)}"
    )

    print(
        source_state["operation"]
        .value_counts()
    )

    sys.exit(0)


# -----------------------
# SUBSEQUENT RUNS
# -----------------------

source_state = pd.read_parquet(
    SOURCE_STATE
)

events = []


# -----------------------
# UPDATE
# -----------------------

update_count = max(
    1,
    int(len(source_state) * 0.08)
)

update_indexes = random.sample(
    list(source_state.index),
    update_count
)


for index in update_indexes:

    current = source_state.loc[index].copy()

    current["fare_amount"] = round(
        current["fare_amount"] * 1.10,
        2
    )

    current["tip_amount"] = round(
        current["tip_amount"] + 2.00,
        2
    )

    current["total_amount"] = round(
        current["total_amount"] + 2.00,
        2
    )

    current["operation"] = "UPDATE"
    current["updated_at"] = datetime.now(UTC)
    current["ingestion_time"] = datetime.now(UTC)

    events.append(
        current.to_dict()
    )

    source_state.loc[index] = current


# -----------------------
# DELETE
# -----------------------

remaining_indexes = [
    i
    for i in source_state.index
    if i not in update_indexes
]

delete_count = max(
    1,
    int(len(source_state) * 0.02)
)

delete_indexes = random.sample(
    remaining_indexes,
    min(delete_count, len(remaining_indexes))
)


for index in delete_indexes:

    current = source_state.loc[index].copy()

    delete_event = current.copy()

    delete_event["operation"] = "DELETE"
    delete_event["updated_at"] = datetime.now(UTC)
    delete_event["ingestion_time"] = datetime.now(UTC)

    events.append(
        delete_event.to_dict()
    )

    source_state = source_state.drop(index)


# -----------------------
# INSERT
# -----------------------

available_new = df[
    ~df["trip_id"].isin(
        source_state["trip_id"]
    )
]

insert_count = min(
    4,
    len(available_new)
)

if insert_count > 0:

    new_rows = available_new.sample(
        n=insert_count
    )

    for _, row in new_rows.iterrows():

        event = row.to_dict()

        event["driver_id"] = random.randint(1, 1000)
        event["rider_id"] = random.randint(1, 5000)
        event["surge_multiplier"] = random.choice(
            [1.0, 1.2, 1.5]
        )

        event["operation"] = "INSERT"
        event["updated_at"] = datetime.now(UTC)
        event["ingestion_time"] = datetime.now(UTC)
        event["source"] = "nyc_taxi"

        events.append(event)

        source_state = pd.concat(
            [
                source_state,
                pd.DataFrame([event])
            ],
            ignore_index=True
        )


# -----------------------
# Save State + CDC
# -----------------------

source_state.to_parquet(
    SOURCE_STATE,
    index=False
)

bronze_df = pd.DataFrame(events)

bronze_df.to_parquet(
    CDC_OUTPUT,
    index=False
)


print(
    f"CDC batch generated with "
    f"{len(bronze_df)} events."
)

print(
    bronze_df["operation"].value_counts()
)