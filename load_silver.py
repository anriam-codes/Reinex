import pandas as pd
from sqlalchemy import create_engine

df = pd.read_parquet("silver/trips_clean.parquet")

engine = create_engine(
    "postgresql://rei:rei123@127.0.0.1:5433/reinex"
)

df.to_sql(
    "silver_trips",
    engine,
    if_exists="replace",
    index=False
)

print("Silver loaded into PostgreSQL")