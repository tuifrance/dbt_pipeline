{{ config(materialized = 'table') }} with date_range as (
  select 
    '20210101' as start_date, 
    format_date(
      '%Y%m%d', 
      date_sub(
        current_date(), 
        interval 1 day
      )
    ) as end_date
) 
select 
  distinct parse_date('%Y%m%d', date) as date, 
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
      x.index = 29
  ) as code_produit, 
  (
    SELECT 
      x.value 
    FROM 
      UNNEST(h.customDimensions) as x 
    WHERE 
      x.index = 35
  ) as nom_produit, 
  (
    SELECT 
      x.value 
    FROM 
      UNNEST(h.customDimensions) as x 
    WHERE 
      x.index = 53
  ) as destination, 
  (
    SELECT 
      x.value 
    FROM 
      UNNEST(h.customDimensions) as x 
    WHERE 
      x.index = 77
  ) as ville_depart, 
  (
    SELECT 
      x.value 
    FROM 
      UNNEST(h.customDimensions) as x 
    WHERE 
      x.index = 81
  ) as ville_arrive, 
  (
    SELECT 
      x.value 
    FROM 
      UNNEST(h.customMetrics) as x 
    WHERE 
      x.index = 8
  ) as pax_total, 
  (
    SELECT 
      x.value 
    FROM 
      UNNEST(h.customMetrics) as x 
    WHERE 
      x.index = 3
  ) as pax_adult, 
  (
    SELECT 
      x.value 
    FROM 
      UNNEST(h.customMetrics) as x 
    WHERE 
      x.index = 4
  ) as pax_enfant, 
  (
    SELECT 
      x.value 
    FROM 
      UNNEST(h.customMetrics) as x 
    WHERE 
      x.index = 5
  ) as pax_bebe, 
  (
    SELECT 
      x.value 
    FROM 
      UNNEST(h.customMetrics) as x 
    WHERE 
      x.index = 10
  ) as duree_sejour, 
  (
    SELECT 
      x.value 
    FROM 
      UNNEST(h.customDimensions) as x 
    WHERE 
      x.index = 42
  ) as continent, 
  (
    SELECT 
      x.value 
    FROM 
      UNNEST(h.customDimensions) as x 
    WHERE 
      x.index = 72
  ) as type_paiement, 
from 
  {{ source('ga_tui_fr', 'ga_sessions_*') }}, 
  date_range, 
  unnest (hits) as h 
where 
  _table_suffix between start_date 
  and end_date 
  and h.transaction.transactionId is not null 
group by 
  1, 
  2, 
  3, 
  4, 
  5, 
  6, 
  7, 
  8, 
  9, 
  10, 
  11, 
  12, 
  13, 
  14, 
  15, 
  16, 
  17 
order by 
  3 asc
