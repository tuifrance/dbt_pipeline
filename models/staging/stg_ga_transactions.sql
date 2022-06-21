{{
  config(
    materialized = 'incremental',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

with
    date_range as (
        select
            format_date(
                '%Y%m%d', date_sub(current_date(), interval 10 day)
            ) as start_date,
            format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) as end_date
    ),

    consolidation as (
        select 
  distinct
            parse_date('%Y%m%d', date) as date,
            device.devicecategory as device,
            channelgrouping,
            (
                select x.value from unnest(h.customdimensions) x where x.index = 25
            ) as type_voyage,
            h.eventinfo.eventcategory,
            h.eventinfo.eventaction,
            count(
                distinct concat(fullvisitorid, cast(visitstarttime as string))
            ) as sessions,
            h.transaction.transactionid as dossier,
            (h.transaction.transactionrevenue) / 1000000 as revenue,
            (
                select x.value from unnest(h.customdimensions) as x where x.index = 29
            ) as code_produit,
            (
                select x.value from unnest(h.customdimensions) as x where x.index = 35
            ) as nom_produit,
            (
                select x.value from unnest(h.customdimensions) as x where x.index = 53
            ) as destination,
            (
                select x.value from unnest(h.customdimensions) as x where x.index = 113
            ) as ville_depart,
            (
                select x.value from unnest(h.customdimensions) as x where x.index = 124
            ) as ville_arrive,
            (
                select x.value from unnest(h.custommetrics) as x where x.index = 8
            ) as pax_total,
            (
                select x.value from unnest(h.custommetrics) as x where x.index = 3
            ) as pax_adult,
            (
                select x.value from unnest(h.custommetrics) as x where x.index = 4
            ) as pax_enfant,
            (
                select x.value from unnest(h.custommetrics) as x where x.index = 5
            ) as pax_bebe,
            (
                select x.value from unnest(h.custommetrics) as x where x.index = 10
            ) as duree_sejour,
            (
                select x.value from unnest(h.customdimensions) as x where x.index = 91
            ) as continent,
            (
                select x.value from unnest(h.customdimensions) as x where x.index = 72
            ) as type_paiement,
            (
                select x.value from unnest(h.custommetrics) as x where x.index = 36
            ) as prixproduit,
        from
            {{ source("ga_tui_fr", "ga_sessions_*") }} as ga,
            date_range,
            unnest(ga.hits) as h
        where
            _table_suffix between start_date
            and end_date
            and h.transaction.transactionid is not null
        group by
            1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
        order by 3 asc
    )

select *
from consolidation
{% if is_incremental() %} where date > (select max(date) from {{ this }}) {% endif %}
order by date desc
