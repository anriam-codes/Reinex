import pandas as pd

df = pd.read_parquet("bronze/trips.parquet")

#clean
df = df.dropna()
df = df.drop_duplicates()

# create trip_duration
df["trip_duration"] = (
    df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
).dt.total_seconds()

df.to_parquet("silver/trips_clean.parquet", index=False)

print("Silver layer created with", len(df), "records")