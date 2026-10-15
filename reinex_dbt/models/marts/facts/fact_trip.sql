
{{ config(materialized='view') }}

select
    trip_id,
    vendor_id,
    passenger_count,

    pickup_location_id,
    dropoff_location_id,

    date(pickup_datetime) as date_key,

    pickup_datetime,
    dropoff_datetime,
    trip_duration_sec,

    trip_distance,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,

    extra,
    mta_tax,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,

    payment_type,
    rate_code_id,
    store_and_fwd_flag

from {{ ref('stg_trips') }}
