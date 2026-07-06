select
    "LocationID" as location_id,
    "Borough" as borough,
    "Zone" as zone,
    service_zone
from {{ source('silver', 'taxi_zone_lookup') }}