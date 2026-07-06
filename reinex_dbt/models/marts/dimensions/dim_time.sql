select distinct
    pickup_datetime::date as date_key,

    extract(year from pickup_datetime) as year,
    extract(month from pickup_datetime) as month,
    extract(day from pickup_datetime) as day,
    extract(hour from pickup_datetime) as hour,

    extract(dow from pickup_datetime) as weekday,

    case
        when extract(dow from pickup_datetime) in (0, 6)
        then true
        else false
    end as is_weekend

from {{ ref('stg_trips') }}