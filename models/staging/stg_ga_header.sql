{{
  config(
    materialized = 'incremental',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

with
date_range as (
    select
        format_date(
            '%Y%m%d', date_sub(current_date(), interval 10 day)
        ) as start_date,
        format_date('%Y%m%d', date_sub(current_date(), interval 1 day))
            as end_date
),

consolidation as (
    select
        parse_date('%Y%m%d', date) as date,
        device.devicecategory,
        channelgrouping,
        h.eventinfo.eventcategory,
        h.eventinfo.eventaction,
        h.eventinfo.eventlabel
    from {{ source("ga_tui_fr", "ga_sessions_*") }}, date_range,
        unnest(hits) as h
    where
        _table_suffix between start_date
        and end_date
        and h.eventinfo.eventcategory = 'Home Pages - Zones de Clic'
        and h.eventinfo.eventaction = 'HP-General-header'
    group by 1, 2, 3, 4, 5, 6
)

select *
from consolidation
{% if is_incremental() %}
    where date > (select max(date) from {{ this }})
{% endif %}
order by date desc
