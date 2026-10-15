
import os
import pandas as pd
from sqlalchemy import create_engine, text

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5433")
BRONZE_PATH = "bronze/trips.parquet"

engine = create_engine(
    f"postgresql://rei:rei123@{DB_HOST}:{DB_PORT}/reinex"
)


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

    is_delete = df["operation"].eq("DELETE")
    is_upsert = df["operation"].isin(["INSERT", "UPDATE"])

    valid_upsert = (
        is_upsert
        & (df["fare_amount"] > 0)
        & (df["trip_distance"] > 0)
        & df["pickup_datetime"].notna()
        & df["dropoff_datetime"].notna()
        & (
            (df["dropoff_datetime"] - df["pickup_datetime"])
            .dt.total_seconds() > 0
        )
    )

    df = df[is_delete | valid_upsert].copy()

    df["trip_duration_sec"] = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds()
    df["processed_time"] = pd.Timestamp.now(tz="UTC")

    return df


def main():
    df = transform_events(pd.read_parquet(BRONZE_PATH))

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
                    text("""
                        DELETE FROM silver_trips
                        WHERE trip_id = :trip_id
                    """),
                    {"trip_id": trip_id}
                )
                continue

            if operation not in ("INSERT", "UPDATE"):
                raise ValueError(
                    f"Unsupported CDC operation: {operation}"
                )

            data = row.drop(labels=["operation"]).to_dict()
            columns = list(data.keys())

            col_sql = ", ".join(f'"{c}"' for c in columns)
            val_sql = ", ".join(f":{c}" for c in columns)
            updates = ", ".join(
                f'"{c}" = EXCLUDED."{c}"'
                for c in columns if c != "trip_id"
            )

            conn.execute(text(f"""
                INSERT INTO silver_trips ({col_sql})
                VALUES ({val_sql})
                ON CONFLICT (trip_id)
                DO UPDATE SET {updates}
            """), data)

    print(f"CDC events validated: {len(df)}")
    print("CDC batch applied to PostgreSQL.")


if __name__ == "__main__":
    main()