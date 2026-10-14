{% snapshot trip_history %}

{{
    config(
        unique_key='trip_id',
        strategy='check',
        check_cols=[
            'driver_id',
            'rider_id',
            'pickup_location_id',
            'dropoff_location_id',
            'fare_amount',
            'tip_amount',
            'tolls_amount',
            'total_amount',
            'trip_distance',
            'trip_duration_sec',
            'payment_type',
            'surge_multiplier'
        ],
        invalidate_hard_deletes=True
    )
}}

SELECT
    trip_id,
    driver_id,
    rider_id,
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    trip_distance,
    trip_duration_sec,
    payment_type,
    surge_multiplier
FROM {{ source('silver', 'silver_trips') }}

{% endsnapshot %}