-- Singular test: no event can be timestamped more than an hour in the future.
select * from {{ ref('stg_events') }} where event_time > now() + interval 1 hour
