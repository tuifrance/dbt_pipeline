{{
    config(
        materialized='incremental',
        labels={
            'type': 'google_analytics',
            'contains_pie': 'no',
            'category': 'production',
        },
    )
}}
with
    date_range as (
        select
           '20220101' as start_date,
            --format_date('%Y%m%d', date_sub(current_date(), interval 10 day)) as start_date,
            format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) as end_date
    )

select 
    distinct
    date,
    visitid,
    clientid,
    fullvisitorid,
    visitstarttime,
    concat(
        date, '_', visitid, '_', clientid, '_', fullvisitorid, '_', visitstarttime
    ) as unique_visit_id,
    h.type,
    h.eventinfo.eventcategory,
    h.eventinfo.eventaction,
    h.eventinfo.eventlabel,
from  {{ source('ga_tui_fr', 'ga_sessions_*') }} as ga,
date_range, 
unnest(ga.hits) as h
where h.eventinfo.eventcategory = 'AB Tasty'
