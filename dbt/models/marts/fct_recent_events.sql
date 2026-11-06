-- Last 7 days, for the map.
select event_id, event_time, mag, mag_class, place, region, lat, lon, depth_km, depth_class, tsunami, alert, felt, url
from {{ ref('stg_events') }}
where event_time >= (select max(event_time) from {{ ref('stg_events') }}) - interval 7 day
order by event_time desc
