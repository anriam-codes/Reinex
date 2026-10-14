
import pandas as pd
from datetime import datetime, UTC
from pathlib import Path

BRONZE_PATH = Path("bronze/trips.parquet")
SILVER_PATH = Path("silver/trips_clean.parquet")


def transform_events(df):
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"], errors="coerce"
    )
    df["tpep_dropoff_datetime"] = pd.to_datetime(
        df["tpep_dropoff_datetime"], errors="coerce"
    )

    df = df.rename(columns={
        "vendorid": "vendor_id",
        "ratecodeid": "rate_code_id",
        "pulocationid": "pickup_location_id",
        "dolocationid": "dropoff_location_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
    })

    valid_upserts = (
        df["operation"].isin(["INSERT", "UPDATE"])
        & (df["fare_amount"] > 0)
        & (df["trip_distance"] > 0)
        & df["pickup_datetime"].notna()
        & df["dropoff_datetime"].notna()
    )

    deletes = df["operation"].eq("DELETE")
    df = df[valid_upserts | deletes].copy()

    upserts = df["operation"].isin(["INSERT", "UPDATE"])
    df["trip_duration_sec"] = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds()

    df = df[
        ~upserts | (df["trip_duration_sec"] > 0)
    ].copy()

    return df


def apply_cdc(events):
    if SILVER_PATH.exists():
        state = pd.read_parquet(SILVER_PATH)
    else:
        state = pd.DataFrame()

    if "operation" in state.columns:
        state = state.drop(columns=["operation"])

    # Preserve event order so Silver follows the same CDC semantics
    # as the PostgreSQL loader.
    for _, event in events.iterrows():
        operation = event["operation"]
        trip_id = event["trip_id"]

        if operation in ("INSERT", "UPDATE"):
            record = event.drop(labels=["operation"]).to_dict()
            record["processed_time"] = datetime.now(UTC)

            if not state.empty:
                state = state[state["trip_id"] != trip_id]

            state = pd.concat(
                [state, pd.DataFrame([record])],
                ignore_index=True
            )

        elif operation == "DELETE" and not state.empty:
            state = state[state["trip_id"] != trip_id]

    return state.drop_duplicates(
        subset=["trip_id"], keep="last"
    ).reset_index(drop=True)


def main():
    events = transform_events(pd.read_parquet(BRONZE_PATH))
    state = apply_cdc(events)

    print(f"Events processed: {len(events)}")
    print(f"Silver rows: {len(state)}")
    print(f"Duplicate trip IDs: {state['trip_id'].duplicated().sum()}")

    SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
    state.to_parquet(SILVER_PATH, index=False)


if __name__ == "__main__":
    main()
