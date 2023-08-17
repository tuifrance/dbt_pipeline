{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}
with ga_consolidation as (
    select
        ga_date as date,
        customga_channel as channelgrouping,
        sum(ga_sessions) as sessions,
        sum(ga_new_users) as new_users,
        sum(ga_bounces) as bounces,
        sum(ga_transactions) as nb_transaction,
        sum(ga_revenue) as revenue
    --sum(unique_sessions) as unique_sessions , 
    --sum(searches) as searches, 
    --sum(product_page) as product_page,       
    --sum(users) as users,

    from {{ ref('stg_ga_funnel_sessions_daily') }}
    group by 1, 2
    order by 1 desc
)

select
    date,
    channelgrouping,
    sessions,
    nb_transaction as transactions,
    revenue,
    new_users,
    bounces,
    sum(sessions) over (partition by date) as g_sessions,
    safe_divide(sessions, sum(sessions) over (partition by date))
        as poids_sessions,
    sum(nb_transaction) over (partition by date) as g_transactions,
    safe_divide(nb_transaction, sum(nb_transaction) over (partition by date))
        as poids_transactions,
    sum(revenue) over (partition by date) as g_revenue,
    safe_divide(revenue, sum(revenue) over (partition by date)) as poids_revenue
from ga_consolidation
order by 1 desc
