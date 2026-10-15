with locations as (

    select pickup_location_id as location_id
    from {{ ref('stg_trips') }}

    union distinct

    select dropoff_location_id as location_id
    from {{ ref('stg_trips') }}

)

select location_id
from locations
where location_id is not null