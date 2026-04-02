import pandas as pd
df = pd.read_parquet("silver/trips_clean.parquet")

print(df.head())
print(df.columns)
print(len(df))


# # print("\nBRONZE")
# # bronze = pd.read_parquet("bronze/trips.parquet")
# # for i, row in bronze.iterrows():
# #     print(row.to_dict())

# print("\nSILVER")
# silver = pd.read_parquet("silver/trips_clean.parquet")
# for i, row in silver.iterrows():
#     print(row.to_dict())