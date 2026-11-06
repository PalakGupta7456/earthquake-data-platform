-- Activity per region over the full history and the last 30 days.
select
    region,
    count(*)                                                            as events_total,
    count(*) filter (where event_time >= now() - interval 30 day)       as events_30d,
    count(*) filter (where mag >= 5)                                    as events_m5,
    max(mag)                                                            as max_mag,
    round(avg(depth_km), 1)                                             as avg_depth_km,
    round(avg(mag), 2)                                                  as avg_mag,
    min(event_time)                                                     as first_event,
    max(event_time)                                                     as last_event
from {{ ref('stg_events') }}
group by region
having count(*) >= 10
order by events_total desc
