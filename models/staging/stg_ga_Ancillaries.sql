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
            parse_date('%Y%m%d', date) as date,
            p.productbrand as marque_produit,
            p.v2productname as product,
            count(h.transaction.transactionid) as achats_uniques,
            sum(p.productquantity) as quantite,
            sum(p.productrevenue) / 1000000 as revenue_produit,
            round(
                sum(p.productrevenue) / 1000000 / sum(p.productquantity), 2
            ) as prix_moy,
        from
            {{ source("ga_tui_fr", "ga_sessions_*") }} as ga,
            date_range,
            unnest(ga.hits) as h,
            unnest(h.product) as p
        where
            totals.visits = 1
            and h.ecommerceaction.action_type = '6'
            and productbrand = 'Ancillaries'
        -- and p.v2productname= 'bagages supplémentaires'
        group by 1, 2, 3
        order by revenue_produit
    )

select *
from consolidation
{% if is_incremental() %} where date > (select max(date) from {{ this }}) {% endif %}
order by date desc
