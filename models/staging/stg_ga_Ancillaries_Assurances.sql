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
        format_date('%Y%m%d', date_sub(current_date(), interval 1 day))
            as end_date
),

consolidation as (
    select
        parse_date('%Y%m%d', date) as date,
        p.productbrand as marque_produit,
        p.v2productname as product,
        h.transaction.transactionid as dossier,
        sum(p.productquantity) as quantite,
        sum(p.productrevenue) / 1000000 as revenue_produit

    from
        {{ source("ga_tui_fr", "ga_sessions_*") }},
        date_range,
        unnest(ga.hits) as h,
        unnest(h.product) as p
    where
        totals.visits = 1
        and h.ecommerceaction.action_type = '6'
        and h.transaction.transactionid is not null
        and productbrand = 'Ancillaries'
        and p.v2productname like 'Assurance%'
        or p.v2productname like 'bagages supplémentaires'

    group by 1, 2, 3, 4
)

select *
from consolidation
{% if is_incremental() %}
    where date > (select max(date) from {{ this }})
{% endif %}
order by date desc
