{{ config(materialized='table') }}
with date_range as (select '20220101' as start_date,
format_date('%Y%m%d',date_sub(current_date(),interval 1 day)) as end_date
)
select Parse_date('%Y%m%d',date) as Date,
device.deviceCategory,
channelGrouping,
(SELECT x.value FROM UNNEST(h.customDimensions) x WHERE x.index = 25) as type_voyage,
 h.eventInfo.eventCategory,
 h.eventInfo.eventAction,
 count(distinct CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))) as sessions,

count(*) as nb_clics,
(SELECT x.value FROM UNNEST(h.customDimensions) as x WHERE x.index = 29) as code_produit,
(SELECT x.value FROM UNNEST(h.customDimensions) as x WHERE x.index = 119) as nom_produit,
(SELECT x.value FROM UNNEST(h.customDimensions) as x WHERE x.index = 41) as destination,
(SELECT x.value FROM UNNEST(h.customDimensions) as x WHERE x.index = 33) as ville_depart,
(SELECT x.value FROM UNNEST(h.customDimensions) as x WHERE x.index = 81) as ville_arrive,
(SELECT x.value FROM UNNEST(h.customDimensions) as x WHERE x.index = 70) as type_produit,
(SELECT x.value FROM UNNEST(h.customDimensions) as x WHERE x.index = 22) as date_depart,
(SELECT x.value FROM UNNEST(h.customDimensions) as x WHERE x.index = 24) as date_arrive,
(SELECT x.value FROM UNNEST(h.customMetrics) as x WHERE x.index = 36) as PrixProduit,
From {{ source('ga_tui_fr', 'ga_sessions_*') }},
date_range,
Unnest (hits) as h
where _table_suffix between start_date and end_date  and h.eventInfo.eventAction='Add to cart' 
group by 1,2,3,4,5,6,9,10,11,12,13,14,15,16,17