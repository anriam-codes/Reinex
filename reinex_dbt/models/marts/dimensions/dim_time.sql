
select distinct
    date(pickup_datetime) as date_key,

    extract(year from pickup_datetime) as year,
    extract(quarter from pickup_datetime) as quarter,
    extract(month from pickup_datetime) as month,
    extract(day from pickup_datetime) as day,

    {% if target.type == 'bigquery' %}
        extract(dayofweek from pickup_datetime) as weekday,

        extract(dayofweek from pickup_datetime) in (1, 7)
            as is_weekend
    {% else %}
        extract(dow from pickup_datetime) + 1 as weekday,

        (extract(dow from pickup_datetime) in (0, 6))
            as is_weekend
    {% endif %}

from {{ ref('stg_trips') }}
where pickup_datetime is not null
