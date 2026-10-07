import pandas as pd
from datetime import datetime, UTC
from pathlib import Path


BRONZE_PATH = "bronze/trips.parquet"
SILVER_PATH = "silver/trips_clean.parquet"


# -----------------------
# Load Bronze
# -----------------------

df = pd.read_parquet(BRONZE_PATH)


# -----------------------
# Standardization
# -----------------------

def standardize(df):

    df.columns = [
        col.lower()
        for col in df.columns
    ]

    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"]
    )

    df["tpep_dropoff_datetime"] = pd.to_datetime(
        df["tpep_dropoff_datetime"]
    )

    return df


# -----------------------
# Rename Columns
# -----------------------

def rename_columns(df):

    return df.rename(
        columns={
            "vendorid": "vendor_id",
            "ratecodeid": "rate_code_id",
            "pulocationid": "pickup_location_id",
            "dolocationid": "dropoff_location_id",
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
        }
    )


# -----------------------
# Validate CDC Events
# -----------------------

def validate_events(df):

    deletes = df[
        df["operation"] == "DELETE"
    ].copy()

    upserts = df[
        df["operation"].isin(
            ["INSERT", "UPDATE"]
        )
    ].copy()

    upserts = upserts[
        (upserts["fare_amount"] > 0)
        & (upserts["trip_distance"] > 0)
    ]

    upserts = upserts.dropna(
        subset=[
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "fare_amount",
            "trip_distance",
        ]
    )

    return pd.concat(
        [
            upserts,
            deletes
        ],
        ignore_index=True
    )


# -----------------------
# Enrichment
# -----------------------

def enrich(df):

    mask = df["operation"].isin(
        ["INSERT", "UPDATE"]
    )

    df.loc[mask, "trip_duration_sec"] = (
        df.loc[mask, "dropoff_datetime"]
        - df.loc[mask, "pickup_datetime"]
    ).dt.total_seconds()

    invalid = (
        mask
        & (
            df["trip_duration_sec"].isna()
            | (df["trip_duration_sec"] <= 0)
        )
    )

    return df[~invalid]


# -----------------------
# Apply CDC
# -----------------------

def apply_cdc(events):

    silver_path = Path(
        SILVER_PATH
    )

    if silver_path.exists():

        current_state = pd.read_parquet(
            silver_path
        )

    else:

        current_state = pd.DataFrame()


    upserts = events[
        events["operation"].isin(
            ["INSERT", "UPDATE"]
        )
    ].copy()

    deletes = events[
        events["operation"] == "DELETE"
    ].copy()


    # Remove CDC-only column
    if "operation" in current_state.columns:

        current_state = current_state.drop(
            columns=["operation"]
        )


    # -----------------------
    # INSERT / UPDATE
    # -----------------------

    if not upserts.empty:

        upserts = upserts.drop_duplicates(
            subset=["trip_id"],
            keep="last"
        )

        if not current_state.empty:

            current_state = current_state[
                ~current_state["trip_id"].isin(
                    upserts["trip_id"]
                )
            ]

        current_state = pd.concat(
            [
                current_state,
                upserts
            ],
            ignore_index=True
        )


    # -----------------------
    # DELETE
    # -----------------------

    if (
        not deletes.empty
        and not current_state.empty
    ):

        current_state = current_state[
            ~current_state["trip_id"].isin(
                deletes["trip_id"]
            )
        ]


    return current_state


# -----------------------
# Metadata
# -----------------------

def add_metadata(df):

    df["processed_time"] = datetime.now(
        UTC
    )

    return df


# -----------------------
# Data Quality
# -----------------------

def data_quality_report(df):

    print(
        "\n------ Data Quality Report ------"
    )

    print(
        f"Rows               : {len(df)}"
    )

    print(
        f"Duplicate Trips    : "
        f"{df['trip_id'].duplicated().sum()}"
    )

    print("\nNull Percentage")

    print(
        (df.isnull().mean() * 100).round(2)
    )

    print("---------------------------------\n")


# -----------------------
# Pipeline
# -----------------------

df = standardize(df)

df = validate_events(df)

df = rename_columns(df)

df = enrich(df)

silver_df = apply_cdc(df)

silver_df = add_metadata(
    silver_df
)

data_quality_report(
    silver_df
)

silver_df.to_parquet(
    SILVER_PATH,
    index=False
)

print(
    f"Silver current state: "
    f"{len(silver_df)} records."
)