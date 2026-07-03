select *
from {{ source('silver', 'silver_trips') }}