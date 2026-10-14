
import os
import pandas as pd
from sqlalchemy import create_engine, text

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5433")

engine = create_engine(
    f"postgresql://rei:rei123@{DB_HOST}:{DB_PORT}/reinex"
)

BRONZE_PATH = "bronze/trips.parquet"


def main():
    df = pd.read_parquet(BRONZE_PATH)
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

    upserts = df["operation"].isin(["INSERT", "UPDATE"])
    valid = (
        df["operation"].eq("DELETE")
        | (
            upserts
            & (df["fare_amount"] > 0)
            & (df["trip_distance"] > 0)
            & df["pickup_datetime"].notna()
            & df["dropoff_datetime"].notna()
            & (
                (df["dropoff_datetime"] - df["pickup_datetime"])
                .dt.total_seconds() > 0
            )
        )
    )
    df = df[valid].copy()

    df["trip_duration_sec"] = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds()
    df["processed_time"] = pd.Timestamp.now(tz="UTC")

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS
            idx_silver_trips_trip_id
            ON silver_trips (trip_id)
        """))

        for _, row in df.iterrows():
            operation = row["operation"]
            trip_id = row["trip_id"]

            if operation == "DELETE":
                conn.execute(
                    text("DELETE FROM silver_trips WHERE trip_id = :id"),
                    {"id": trip_id}
                )
                continue

            data = row.drop(labels=["operation"]).to_dict()
            columns = list(data.keys())

            if operation == "INSERT":
                col_sql = ", ".join(f'"{c}"' for c in columns)
                val_sql = ", ".join(f":{c}" for c in columns)
                updates = ", ".join(
                    f'"{c}" = EXCLUDED."{c}"'
                    for c in columns if c != "trip_id"
                )

                conn.execute(text(f"""
                    INSERT INTO silver_trips ({col_sql})
                    VALUES ({val_sql})
                    ON CONFLICT (trip_id) DO UPDATE SET {updates}
                """), data)

            elif operation == "UPDATE":
                assignments = ", ".join(
                    f'"{c}" = :{c}'
                    for c in columns if c != "trip_id"
                )
                conn.execute(text(f"""
                    UPDATE silver_trips
                    SET {assignments}
                    WHERE trip_id = :trip_id
                """), data)

    print("CDC batch applied to PostgreSQL.")


if __name__ == "__main__":
    main()
