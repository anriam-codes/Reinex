import pandas as pd

df = pd.read_parquet("silver/trips_clean.parquet")

print(df.head())
print(df.columns)
print(len(df))