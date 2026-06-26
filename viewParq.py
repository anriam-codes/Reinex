#scripts to view bronze and silver lakes
import pandas as pd

df = pd.read_parquet("silver/trips_clean.parquet")

print(df.head())
print(df.columns)
print(len(df))

print("\nBRONZE")
for _, row in df.iterrows():
    print(row.to_dict())

# Uncomment only if silver file exists
# print("\nSILVER")
# silver = pd.read_parquet("silver/trips_clean.parquet")
# for _, row in silver.iterrows():
#     print(row.to_dict())