from sqlalchemy import create_engine, text, inspect
import pandas as pd
import os

df = pd.read_parquet("silver/trips_clean.parquet")

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5433")

engine = create_engine(
    f"postgresql://rei:rei123@{DB_HOST}:{DB_PORT}/reinex"
)

inspector = inspect(engine)

if inspector.has_table("silver_trips"):
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE silver_trips CASCADE"))

df.to_sql(
    "silver_trips",
    engine,
    if_exists="append",
    index=False
)

print("Silver loaded into PostgreSQL")