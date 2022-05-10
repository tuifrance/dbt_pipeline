{{ config(materialized = 'table') }}
 with date_range as (
 select 
    '20220101' as start_date, 
    format_date(
      '%Y%m%d', 
      date_sub(
        current_date(), 
        interval 1 day
      )
    ) as end_date
) ,
data as ( select 
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
  )

select 
 count( distinct case when type_paiement is null then dossier  end) Null_dossier,
 sum( case when type_paiement is null then revenue  end) Null_revenue,

from data
 