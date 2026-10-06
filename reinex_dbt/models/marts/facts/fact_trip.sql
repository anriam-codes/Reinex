{{ config(
    materialized='incremental',
    unique_key='trip_id'
) }}

select
    trip_id,
    driver_id,
    rider_id,
    pickup_location_id,
    dropoff_location_id,

    pickup_datetime::date as date_key,

    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,

    trip_distance,
    trip_duration_sec,

    payment_type,
    surge_multiplier

from {{ ref('stg_trips') }}

{% if is_incremental() %}

where trip_id not in (
    select trip_id
    from {{ this }}
)

{% endif %}