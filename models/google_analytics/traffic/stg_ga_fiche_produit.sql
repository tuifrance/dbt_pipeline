{{
  config(
    materialized = 'incremental',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}
with
    date_range as (
        select
            '20210101' as start_date,         
            --format_date('%Y%m%d', date_sub(current_date(), interval 10 day)) as start_date,
            format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) as end_date
    ),
    data as (
        select distinct
    parse_date('%Y%m%d', date) as date,
    device.devicecategory as device,
    channelgrouping,
    trafficsource.campaign,
    trafficsource.medium,
    trafficsource.source,
    trafficSource.keyword, 
    trafficSource.adContent,                          
    (select x.value from unnest(h.customdimensions) as x where x.index = 29 ) as code_produit,
    (select x.value from unnest(h.customdimensions) as x where x.index = 119) as nom_produit,
    (select x.value from unnest(h.customdimensions) as x where x.index = 77) as destination,
    (select x.value from unnest(h.customdimensions) x where x.index = 70) as type_voyage,
    count(distinct case when h.eventinfo.eventcategory = 'Code Produit - Fiche Produit' then concat(fullvisitorid, cast(visitstarttime as string)) end) as consultation,
    count( distinct case when h.eventinfo.eventcategory = 'Fiche Produit - Zones de Clic' and h.eventinfo.eventaction = 'Voir les tarifs' then concat(fullvisitorid, cast(visitstarttime as string)) end ) as voir_tarif,
    count( distinct case when h.eventinfo.eventcategory = 'Fiche Produit - Zones de Clic' and h.eventinfo.eventaction = 'FP-calendrier' and h.eventinfo.eventlabel = 'departureCity' then concat(fullvisitorid, cast(visitstarttime as string)) end ) as ville_depart,
    count( distinct case when h.eventinfo.eventcategory = 'Fiche Produit - Zones de Clic' and h.eventinfo.eventaction = 'FP-calendrier' and h.eventinfo.eventlabel = 'duration' then concat(fullvisitorid, cast(visitstarttime as string)) end ) as duration,
    count( distinct case when h.eventinfo.eventcategory = 'Fiche Produit - Zones de Clic' and h.eventinfo.eventaction = 'FP-calendrier' and h.eventinfo.eventlabel = 'goFunnel' then concat(fullvisitorid, cast(visitstarttime as string)) end ) as gofunnel,

from
    {{ source('ga_tui_fr', 'ga_sessions_*') }} as ga,
    date_range,
    unnest(hits) as h
where _table_suffix between start_date and end_date
group by 1, 2, 3, 4, 5, 6, 7,8,9,10,11,12
), 
consolidation as (
select
    date,
    device,
    channelgrouping,
    campaign, 
    source, 
    medium,
    keyword, 
    adContent, 
    code_produit,
    nom_produit,
    destination,
    case when type_voyage = '' then 'other' else type_voyage end as type_voyage,
    consultation,
    voir_tarif,
    ville_depart,
    duration,
    gofunnel
from data
)

select *
from consolidation
{% if is_incremental() %} where date > (select max(date) from {{ this }}) {% endif %}
order by date desc
