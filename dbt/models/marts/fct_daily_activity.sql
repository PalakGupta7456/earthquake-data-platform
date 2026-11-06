-- Daily global activity with a 7-day rolling mean and seismic energy release.
with daily as (
    select
        event_date,
        count(*)                                   as events,
        count(*) filter (where mag >= 4.5)         as events_m45,
        max(mag)                                   as max_mag,
        -- Gutenberg-Richter energy relation: log10 E[J] = 1.5 M + 4.8
        sum(pow(10, 1.5 * mag + 4.8))              as energy_joules
    from {{ ref('stg_events') }}
    group by 1
)
select
    *,
    avg(events) over (order by event_date rows between 6 preceding and current row) as events_7d_avg
from daily
order by event_date
