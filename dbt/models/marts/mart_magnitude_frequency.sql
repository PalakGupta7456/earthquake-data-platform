-- Frequency-magnitude distribution (input for Gutenberg-Richter fits).
select
    region,
    mag_bin,
    count(*)                                                        as n,
    sum(count(*)) over (partition by region order by mag_bin desc)  as n_cumulative
from {{ ref('stg_events') }}
group by region, mag_bin
order by region, mag_bin
