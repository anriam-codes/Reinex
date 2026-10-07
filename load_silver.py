import pandas as pd
from sqlalchemy import create_engine, text
import os


# -----------------------
# Load Bronze CDC Batch
# -----------------------

df = pd.read_parquet(
    "bronze/trips.parquet"
)


# -----------------------
# Standardization
# -----------------------

df.columns = [
    col.lower()
    for col in df.columns
]

df["tpep_pickup_datetime"] = pd.to_datetime(
    df["tpep_pickup_datetime"]
)

df["tpep_dropoff_datetime"] = pd.to_datetime(
    df["tpep_dropoff_datetime"]
)


# -----------------------
# Semantic Renaming
# -----------------------

df = df.rename(
    columns={
        "vendorid": "vendor_id",
        "ratecodeid": "rate_code_id",
        "pulocationid": "pickup_location_id",
        "dolocationid": "dropoff_location_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
    }
)


# -----------------------
# Enrichment
# -----------------------

df["trip_duration_sec"] = (
    df["dropoff_datetime"]
    - df["pickup_datetime"]
).dt.total_seconds()


# -----------------------
# Metadata
# -----------------------

df["processed_time"] = pd.Timestamp.now(tz="UTC")


# -----------------------
# PostgreSQL Connection
# -----------------------

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5433")

engine = create_engine(
    f"postgresql://rei:rei123@{DB_HOST}:{DB_PORT}/reinex"
)


# -----------------------
# Apply CDC
# -----------------------

with engine.begin() as conn:

    for _, row in df.iterrows():

        operation = row["operation"]
        trip_id = row["trip_id"]

        row_data = row.to_dict()


        # -----------------------
        # INSERT
        # -----------------------

        if operation == "INSERT":

            columns = ", ".join(row_data.keys())

            values = ", ".join(
                f":{column}"
                for column in row_data.keys()
            )

            query = text(f"""
                INSERT INTO silver_trips
                ({columns})
                VALUES ({values})
                ON CONFLICT (trip_id)
                DO NOTHING
            """)

            conn.execute(
                query,
                row_data
            )


        # -----------------------
        # UPDATE
        # -----------------------

        elif operation == "UPDATE":

            set_clause = ", ".join(
                f"{column} = :{column}"
                for column in row_data.keys()
                if column != "trip_id"
            )

            query = text(f"""
                UPDATE silver_trips
                SET {set_clause}
                WHERE trip_id = :trip_id
            """)

            conn.execute(
                query,
                row_data
            )


        # -----------------------
        # DELETE
        # -----------------------

        elif operation == "DELETE":

            query = text("""
                DELETE FROM silver_trips
                WHERE trip_id = :trip_id
            """)

            conn.execute(
                query,
                {"trip_id": trip_id}
            )


print("CDC changes applied to PostgreSQL.")