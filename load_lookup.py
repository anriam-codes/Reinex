import pandas as pd
from sqlalchemy import create_engine
import os

df = pd.read_csv("datasets/taxi_zone_lookup.csv")


DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5433")

engine = create_engine(
    f"postgresql://rei:rei123@{DB_HOST}:{DB_PORT}/reinex"
)

df.to_sql(
    "taxi_zone_lookup",
    engine,
    if_exists="replace",
    index=False
)

print("Taxi zones loaded.")