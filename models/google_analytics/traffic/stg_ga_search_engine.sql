{{
  config(
    materialized = 'incremental',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

with
    date_range as (
        select
            --'20210101' as start_date,                     
            format_date('%Y%m%d', date_sub(current_date(), interval 10 day)) as start_date,
            format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) as end_date
    ) , 

    consolidation as (
        select
            parse_date('%Y%m%d', date) as date,
            device.devicecategory as type_appareil,
            channelgrouping as channel,
            trafficsource.campaign,
            trafficsource.medium,
            trafficsource.source,              
            ( select x.value from unnest(h.customdimensions) x where x.index = 41) as destination,
            (select x.value from unnest(h.customdimensions) x where x.index = 33) as ville_depart,
            (select x.value from unnest(h.customdimensions) x where x.index = 22) as date_depart,
            (select x.value from unnest(h.customdimensions) x where x.index = 143) as duree_voyage,
            (select x.value from unnest(h.customdimensions) x where x.index = 25) as type_voyage,
            count(*) as searches
        from {{ source('ga_tui_fr', 'ga_sessions_*') }}, date_range, unnest(hits) as h
        where
            _table_suffix between start_date
            and end_date
            and h.eventinfo.eventcategory = 'Utilisation Moteur HP'
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11
  )

select *
from consolidation
{% if is_incremental() %} where date > (select max(date) from {{ this }}) {% endif %}
order by date desc