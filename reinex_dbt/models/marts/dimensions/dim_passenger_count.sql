select distinct
    passenger_count
from {{ ref('stg_trips') }}
where passenger_count is not null