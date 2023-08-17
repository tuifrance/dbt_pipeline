{{
    config(
        materialized='incremental',
        labels={
            'type': 'google_analytics',
            'contains_pie': 'no',
            'category': 'production',
        }
    )
}}


with
date_range as (
    select
        format_date('%Y%m%d', date_sub(current_date(), interval 10 day))
            as start_date,
        format_date('%Y%m%d', date_sub(current_date(), interval 1 day))
            as end_date
),

consolidation as (
    select
        parse_date('%Y%m%d', date) as date,
        device.devicecategory,
        channelgrouping,
        (
            select x.value
            from unnest(h.customdimensions) as x
            where x.index = 25
        ) as type_voyage,
        h.eventinfo.eventcategory,
        h.eventinfo.eventaction,
        count(distinct concat(fullvisitorid, cast(visitstarttime as string)))
            as sessions,
        count(*) as nb_clics,
        (
            select x.value
            from unnest(h.customdimensions) as x
            where x.index = 29
        ) as code_produit,
        (
            select x.value
            from unnest(h.customdimensions) as x
            where x.index = 119
        ) as nom_produit,
        (
            select x.value
            from unnest(h.customdimensions) as x
            where x.index = 41
        ) as destination,
        (
            select x.value
            from unnest(h.customdimensions) as x
            where x.index = 33
        ) as ville_depart,
        (
            select x.value
            from unnest(h.customdimensions) as x
            where x.index = 81
        ) as ville_arrive,
        (
            select x.value
            from unnest(h.customdimensions) as x
            where x.index = 70
        ) as type_produit,
        (
            select x.value
            from unnest(h.customdimensions) as x
            where x.index = 22
        ) as date_depart,
        (
            select x.value
            from unnest(h.customdimensions) as x
            where x.index = 24
        ) as date_arrive,
        (select x.value from unnest(h.custommetrics) as x where x.index = 36)
            as prixproduit
    from {{ source('ga_tui_fr', 'ga_sessions_*') }}, date_range,
        unnest(ga.hits) as h
    where
        _table_suffix between start_date and end_date
        and h.eventinfo.eventaction = 'Add to cart'
    group by 1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14, 15, 16, 17
)


select *
from consolidation
{% if is_incremental() %}
    where date > (select max(date) from {{ this }})
{% endif %}
order by date desc
