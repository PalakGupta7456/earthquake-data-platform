-- One row per earthquake, with derived classifications used by every mart.
select
    event_id,
    event_time,
    cast(event_time as date)                                   as event_date,
    mag,
    floor(mag * 10) / 10                                       as mag_bin,
    mag_type,
    place,
    region,
    lat,
    lon,
    depth_km,
    case
        when depth_km < 70  then 'shallow'
        when depth_km < 300 then 'intermediate'
        else 'deep'
    end                                                        as depth_class,
    case
        when mag >= 7 then 'major'
        when mag >= 6 then 'strong'
        when mag >= 5 then 'moderate'
        when mag >= 4 then 'light'
        else 'minor'
    end                                                        as mag_class,
    sig,
    tsunami,
    alert,
    status = 'reviewed'                                        as is_reviewed,
    felt,
    url
from {{ source('silver', 'silver_events') }}
