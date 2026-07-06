select distinct
    rider_id
from {{ ref('stg_trips') }}
where rider_id is not null