{{ config(materialized='table') }}
with date_range as (
select
    '20210101' as start_date,
    format_date('%Y%m%d',date_sub(current_date(), interval 1 day)) as end_date 
    ),

 data as (select distinct
   parse_date('%Y%m%d',date) as date, 
   device.deviceCategory as device,
   channelGrouping, 
 
(SELECT x.value FROM UNNEST(h.customDimensions) as x WHERE x.index = 29) as code_produit,
(SELECT x.value FROM UNNEST(h.customDimensions) as x WHERE x.index = 119) as nom_produit,
(SELECT x.value FROM UNNEST(h.customDimensions) as x WHERE x.index = 77) as destination,
(SELECT x.value FROM UNNEST(h.customDimensions) x WHERE x.index = 70) as type_voyage,

count(distinct case when h.eventInfo.eventCategory='Code Produit - Fiche Produit'  then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end) as consultation,
count(distinct case when h.eventInfo.eventCategory='Fiche Produit - Zones de Clic' and h.eventInfo.eventAction= 'Voir les tarifs' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end) as voir_tarif,
count(distinct case when h.eventInfo.eventCategory='Fiche Produit - Zones de Clic' and h.eventInfo.eventAction= 'FP-calendrier' and h.eventInfo.eventLabel= 'departureCity'  then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end) as ville_depart,
count(distinct case when h.eventInfo.eventCategory='Fiche Produit - Zones de Clic' and h.eventInfo.eventAction= 'FP-calendrier' and h.eventInfo.eventLabel= 'duration'  then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end) as duration,
count(distinct case when h.eventInfo.eventCategory='Fiche Produit - Zones de Clic' and h.eventInfo.eventAction= 'FP-calendrier' and h.eventInfo.eventLabel= 'goFunnel'  then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end) as goFunnel,

from  {{ source('ga_tui_fr', 'ga_sessions_*') }},
date_range,
unnest (hits) as h
where _table_suffix between start_date and end_date

group by 1,2,3,4,5,6,7)

select date,device,channelGrouping,code_produit,nom_produit,destination,
case when type_voyage='' then 'other' else type_voyage end as type_voyage ,consultation,voir_tarif,ville_depart,duration,goFunnel
from data

