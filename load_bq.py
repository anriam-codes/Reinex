
from pathlib import Path
import tempfile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from google.cloud import bigquery


PROJECT_ID = "reinex-pro"
DATASET_ID = "reinex"
TABLE_ID = "raw_taxi_trips"

DATA_DIR = Path(__file__).resolve().parent / "datasets"
MONTH_FILES = [
    DATA_DIR / f"2023-{month:02d}.parquet"
    for month in range(1, 13)
]

# Standard schema shared by all 12 monthly files.
CANONICAL_SCHEMA = pa.schema([
    ("VendorID", pa.int64()),
    ("tpep_pickup_datetime", pa.timestamp("us")),
    ("tpep_dropoff_datetime", pa.timestamp("us")),
    ("passenger_count", pa.float64()),
    ("trip_distance", pa.float64()),
    ("RatecodeID", pa.float64()),
    ("store_and_fwd_flag", pa.string()),
    ("PULocationID", pa.int64()),
    ("DOLocationID", pa.int64()),
    ("payment_type", pa.int64()),
    ("fare_amount", pa.float64()),
    ("extra", pa.float64()),
    ("mta_tax", pa.float64()),
    ("tip_amount", pa.float64()),
    ("tolls_amount", pa.float64()),
    ("improvement_surcharge", pa.float64()),
    ("total_amount", pa.float64()),
    ("congestion_surcharge", pa.float64()),
    ("airport_fee", pa.float64()),
])


def normalize_parquet(source, destination):
    table = pq.read_table(source)

    # Normalize column names, including Airport_fee -> airport_fee.
    names = [name.lower() for name in table.column_names]

    if len(names) != len(set(names)):
        raise ValueError(
            f"Duplicate column names after normalization: {source.name}"
        )

    table = table.rename_columns(names)
    arrays = []

    for field in CANONICAL_SCHEMA:
        if field.name in table.column_names:
            column = table[field.name].combine_chunks()

            try:
                column = pc.cast(
                    column,
                    target_type=field.type,
                    safe=False,
                )
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
                raise ValueError(
                    f"Cannot normalize {field.name} in {source.name}: {exc}"
                ) from exc
        else:
            # Some months may not contain every surcharge column.
            column = pa.nulls(
                table.num_rows,
                type=field.type,
            )

        arrays.append(column)

    normalized = pa.Table.from_arrays(
        arrays,
        schema=CANONICAL_SCHEMA,
    )

    pq.write_table(
        normalized,
        destination,
        compression="snappy",
    )

    return normalized.num_rows


def main():
    missing = [str(path) for path in MONTH_FILES if not path.exists()]

    if missing:
        raise FileNotFoundError(
            "Missing monthly files:\n" + "\n".join(missing)
        )

    client = bigquery.Client(project=PROJECT_ID)

    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    table_ref = f"{dataset_ref}.{TABLE_ID}"

    dataset = client.get_dataset(dataset_ref)
    location = dataset.location

    print(f"Dataset: {dataset_ref}")
    print(f"Location: {location}")

    total_local_rows = 0

    with tempfile.TemporaryDirectory(prefix="reinex_bq_") as temp_dir:
        for index, source in enumerate(MONTH_FILES):
            normalized_path = Path(temp_dir) / source.name

            print(f"\nProcessing {source.name}...")

            rows = normalize_parquet(source, normalized_path)
            total_local_rows += rows

            print(f"Normalized rows: {rows:,}")

            disposition = (
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if index == 0
                else bigquery.WriteDisposition.WRITE_APPEND
            )

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=disposition,
            )

            with normalized_path.open("rb") as file:
                job = client.load_table_from_file(
                    file,
                    table_ref,
                    job_config=job_config,
                    location=location,
                )

            job.result()
            print(f"Uploaded rows: {job.output_rows:,}")

    table = client.get_table(table_ref)

    query = f"""
        SELECT COUNT(*) AS row_count
        FROM `{table_ref}`
    """

    result = list(
        client.query(query, location=location).result()
    )

    bigquery_rows = result[0]["row_count"]

    print("\n========== VALIDATION ==========")
    print(f"Local rows:    {total_local_rows:,}")
    print(f"BigQuery rows: {bigquery_rows:,}")
    print(f"Table:         {table.full_table_id}")
    print(f"Columns:       {len(table.schema)}")

    if total_local_rows != bigquery_rows:
        raise RuntimeError(
            "Row counts do not match. Investigate before proceeding."
        )

    print("\nSUCCESS: All 12 monthly files loaded and verified.")


if __name__ == "__main__":
    main()
