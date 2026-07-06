select
    trip_id,

    -- Dimension Keys
    driver_id,
    rider_id,

    pickup_location_id,
    dropoff_location_id,

    pickup_datetime::date as date_key,

    -- Trip Metrics
    passenger_count,
    trip_distance,
    trip_duration_sec,

    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,

    surge_multiplier,

    -- Operational Metrics
    payment_type,
    vendor_id,
    rate_code_id,

    -- Metadata
    ingestion_time,
    processed_time

from {{ ref('stg_trips') }}