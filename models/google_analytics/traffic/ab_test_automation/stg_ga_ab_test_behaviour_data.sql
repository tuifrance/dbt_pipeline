{{
    config(
        materialized='incremental',
        labels={
            'type': 'google_analytics',
            'contains_pie': 'no',
            'category': 'production',
        },
    )
}}
with
    date_range as (
        select
            --'20210101' as start_date, 
            format_date('%Y%m%d', date_sub(current_date(), interval 10 day)) as start_date,
            format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) as end_date
    ) , 

consolidation as (
select 
distinct
    date,
    visitid,
    clientid,
    fullvisitorid,
    visitstarttime,
    concat(
        date, '_', visitid, '_', clientid, '_', fullvisitorid, '_', visitstarttime
    ) as unique_visit_id,
    count(distinct concat(fullvisitorid, cast(visitstarttime as string))) as sessions,
    count(
        distinct case
            when h.eventinfo.eventcategory = 'Utilisation Moteur HP'
            then concat(fullvisitorid, '_', visitid)
        end
    ) as searches,
    count(
        distinct case
            when h.eventinfo.eventcategory = 'Code Produit - Fiche Produit'
            then concat(fullvisitorid, '_', visitid)
        end
    ) as product_page,
    count(
        distinct case
            when h.eventinfo.eventcategory = 'Filtre des pages resultats'
            then concat(fullvisitorid, '_', visitid)
        end
    ) as search_page,
    count(
        distinct case
            when h.eventinfo.eventcategory = 'Fiche Produit - Zones de Clic'
            then concat(fullvisitorid, '_', visitid)
        end
    ) as product_page_clicks,
    count(
        distinct case
            when
                h.contentgroup.contentgroup1 = 'Tunnel'
                and h.contentgroup.contentgroup2 = 'Step2'
            then concat(fullvisitorid, '_', visitid)
        end
    ) as tunnel_step_1,
    count(
        distinct case
            when
                h.contentgroup.contentgroup1 = 'Tunnel'
                and h.contentgroup.contentgroup2 = 'Step2ClientAccount'
            then concat(fullvisitorid, '_', visitid)
        end
    ) as tunnel_step_2,
    count(
        distinct case
            when
                h.contentgroup.contentgroup1 = 'Tunnel'
                and h.contentgroup.contentgroup2 = 'Step3Buyer'
            then concat(fullvisitorid, '_', visitid)
        end
    ) as tunnel_step_3,
    count(
        distinct case
            when
                h.contentgroup.contentgroup1 = 'Tunnel'
                and h.contentgroup.contentgroup2 = 'Step4'
            then concat(fullvisitorid, '_', visitid)
        end
    ) as tunnel_step_4,
    count(
        distinct case
            when totals.bounces = 1
            then concat(fullvisitorid, cast(visitstarttime as string))
        end
    ) as bounces,
    count(
        distinct case
            when totals.newvisits = 1
            then concat(fullvisitorid, cast(visitstarttime as string))
        end
    ) as new_users,
    count(h.transaction.transactionid) as nb_transaction,
    round(sum(h.transaction.transactionrevenue / 1000000), 2) as revenue
from {{ source('ga_tui_fr', 'ga_sessions_*') }} as ga, date_range, unnest(ga.hits) as h
where _table_suffix between start_date and end_date and totals.visits = 1
group by 1, 2, 3, 4, 5, 6
)

select *

from consolidation
{% if is_incremental() %} where date > (select max(date) from {{ this }}) {% endif %}
order by date desc