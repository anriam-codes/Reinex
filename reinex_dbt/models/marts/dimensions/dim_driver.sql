select distinct
    driver_id
from {{ ref('stg_trips') }}
where driver_id is not null