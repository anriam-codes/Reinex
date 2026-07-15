import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv("datasets/taxi_zone_lookup.csv")

engine = create_engine(
    "postgresql://rei:rei123@127.0.0.1:5433/reinex"
)

df.to_sql(
    "taxi_zone_lookup",
    engine,
    if_exists="replace",
    index=False
)

print("Taxi zones loaded.")