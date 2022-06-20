{{
  config(
    materialized = 'incremental',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

with
    date_range as (
        select
            format_date('%Y%m%d', date_sub(current_date(), interval 10 day)) as start_date,
            format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) as end_date
    ), 

consolidation as (
select 
  Parse_date('%Y%m%d', date) as Date, 
  device.deviceCategory, 
  channelGrouping, 
  h.eventInfo.eventCategory, 
  h.eventInfo.eventAction, 
  h.eventInfo.eventLabel, 
From 
  {{ source('ga_tui_fr', 'ga_sessions_*') }}, 
  date_range, 
  Unnest (hits) as h 
where 
  _table_suffix between start_date 
  and end_date 
  and h.eventInfo.eventCategory = 'Home Pages - Zones de Clic' 
  and h.eventInfo.eventAction = 'HP-General-header' 
group by 
  1, 
  2, 
  3, 
  4, 
  5, 
  6)
  
select * from consolidation
{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}
order by date desc 

