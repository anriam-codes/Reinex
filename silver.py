import pandas as pd
from datetime import datetime

# -----------------------
# Load Bronze Layer
# -----------------------

df = pd.read_parquet("bronze/trips.parquet")


# -----------------------
# Standardization
# -----------------------

def standardize(df):
    df.columns = [col.lower() for col in df.columns]

    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"]
    )

    df["tpep_dropoff_datetime"] = pd.to_datetime(
        df["tpep_dropoff_datetime"]
    )

    return df


# -----------------------
# Cleaning
# -----------------------

def clean(df):

    # Remove duplicate business records
    df = df.drop_duplicates(subset=["trip_id"])

    # Remove invalid trips
    df = df[df["fare_amount"] > 0]
    df = df[df["trip_distance"] > 0]

    # Remove incomplete records
    df = df.dropna(
        subset=[
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "fare_amount",
            "trip_distance",
        ]
    )

    return df


# -----------------------
# Semantic Renaming
# -----------------------

def rename_columns(df):

    return df.rename(
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
# Lightweight Enrichment
# -----------------------

def enrich(df):

    df["trip_duration_sec"] = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds()

    df = df[df["trip_duration_sec"] > 0]

    return df


# -----------------------
# Metadata
# -----------------------

def add_metadata(df):

    df["processed_time"] = datetime.utcnow()

    return df


# -----------------------
# Data Quality Report
# -----------------------

def data_quality_report(df):

    print("\n------ Data Quality Report ------")
    print(f"Rows               : {len(df)}")
    print(f"Duplicate Trips    : {df['trip_id'].duplicated().sum()}")
    print("\nNull Percentage")
    print((df.isnull().mean() * 100).round(2))
    print("---------------------------------\n")


# -----------------------
# Pipeline Execution
# -----------------------

df = standardize(df)
df = clean(df)
df = rename_columns(df)
df = enrich(df)
df = add_metadata(df)

data_quality_report(df)

df.to_parquet(
    "silver/trips_clean.parquet",
    index=False
)

print(f"Silver layer created with {len(df)} records.")