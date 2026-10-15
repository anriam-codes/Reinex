select distinct
    vendor_id
from {{ ref('stg_trips') }}
where vendor_id is not null