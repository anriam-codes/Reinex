import pandas as pd
from datetime import datetime

# Load Bronze
df = pd.read_parquet("bronze/trips.parquet")


# 1. Standardization
def standardize(df):
    df.columns = [col.lower() for col in df.columns]
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    return df


# 2. Cleaning
def clean(df):
    df = df.drop_duplicates(subset=["trip_id"])

    df = df[df["fare_amount"] > 0]
    df = df[df["trip_distance"] > 0]

    df = df.dropna(subset=[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "fare_amount",
        "trip_distance"
    ])

    return df


# 3. Feature Engineering
def feature_engineering(df):
    df["trip_duration_sec"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds()

    df = df[df["trip_duration_sec"] > 0]

    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour

    return df


 
# 4. Semantic Renaming
def rename_columns(df):
    df = df.rename(columns={
        "vendorid": "vendor_id",
        "ratecodeid": "rate_code_id",
        "pulocationid": "pickup_location_id",
        "dolocationid": "dropoff_location_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime"
    })
    return df


# 5. Data Quality
def data_quality_report(df):
    print("\n--- Data Quality Report ---")
    print("Total rows:", len(df))
    print("Null %:\n", (df.isnull().sum() / len(df)) * 100)
    print("---------------------------\n")


# Pipeline Execution
df = standardize(df)
df = clean(df)
df = feature_engineering(df)
df = rename_columns(df)

# metadata
df["processed_time"] = datetime.utcnow()

# quality check
data_quality_report(df)

# Save Silver
df.to_parquet(
    "silver/trips_clean.parquet",
    index=False
)

print("Silver layer created with", len(df), "records")