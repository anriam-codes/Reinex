select distinct
    date(pickup_datetime) as date_key,

    extract(year from pickup_datetime) as year,
    extract(quarter from pickup_datetime) as quarter,
    extract(month from pickup_datetime) as month,
    extract(day from pickup_datetime) as day,

    extract(dayofweek from pickup_datetime) as weekday,

    extract(dayofweek from pickup_datetime) in (1, 7)
        as is_weekend

from {{ ref('stg_trips') }}
where pickup_datetime is not null