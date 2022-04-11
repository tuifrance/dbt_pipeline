{{ config(materialized='table') }}
with date_range as (
select
    '20210101' as start_date,
    format_date('%Y%m%d',date_sub(current_date(), interval 1 day)) as end_date 
    )

 select distinct
   parse_date('%Y%m%d',date) as date, 
   channelGrouping, 
   h.transaction.transactionId as dossier, 
   h.transaction.transactionRevenue as revenue
FROM

  {{ source('ga_tui_fr', 'ga_sessions_*') }} AS GA,
  date_range,
  unnest(GA.hits) AS h
where
  _table_suffix between start_date and end_date 
    and h.transaction.transactionId is not null 
    order by 3 asc 