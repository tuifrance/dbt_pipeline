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
  device.deviceCategory as device, 
  channelGrouping, 
  h.transaction.transactionId as dossier, 
  (
    h.transaction.transactionRevenue
  )/ 1000000 as revenue, 
  (
    SELECT 
      x.value 
    FROM 
      UNNEST(h.customDimensions) as x 
    WHERE 
      x.index = 72
  ) as type_paiement, 


From 
  {{ source('ga_tui_fr', 'ga_sessions_*') }}as ga, 
  date_range, 
  unnest (ga.hits) as h 
where 
  _table_suffix between start_date 
  and end_date 
  and h.transaction.transactionId is not null
  )

select * from consolidation
{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}
order by date desc 
  
 