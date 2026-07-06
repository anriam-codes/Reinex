from sqlalchemy import create_engine, text
import pandas as pd

df = pd.read_parquet("silver/trips_clean.parquet")

engine = create_engine(
    "postgresql://rei:rei123@127.0.0.1:5433/reinex"
)

with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE silver_trips CASCADE"))

df.to_sql(
    "silver_trips",
    engine,
    if_exists="append",
    index=False
)

print("Silver loaded into PostgreSQL")  