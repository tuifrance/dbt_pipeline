{{ config(materialized = 'table') }}
 with date_range as (
 select 
    '20211001' as start_date, 
    format_date(
      '%Y%m%d', 
      date_sub(
        current_date(), 
        interval 1 day
      )
    ) as end_date
) 
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
  {{ source('ga_tui_fr', 'ga_sessions_*') }}, 
  date_range, 
  unnest (hits) as h 
where 
  _table_suffix between start_date 
  and end_date 
  and h.transaction.transactionId is not null
  
 