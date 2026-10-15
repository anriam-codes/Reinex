with source_data as (

    select *
    from {{ source('silver', 'silver_trips') }}

),

staged as (

    select
        to_hex(sha256(to_json_string(t))) as trip_id,

        vendorid as vendor_id,
        passenger_count,

        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,

        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,

        payment_type,

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

        ratecodeid as rate_code_id,
        store_and_fwd_flag,

        timestamp_diff(
            tpep_dropoff_datetime,
            tpep_pickup_datetime,
            SECOND
        ) as trip_duration_sec

    from source_data as t

)

select *
from staged