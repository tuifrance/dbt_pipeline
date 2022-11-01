{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}
with ga_consolidation as ( 
select 
      date, 
      customchannelgrouping as channelgrouping, 
      sum(unique_sessions) as unique_sessions , 
      sum(sessions) as sessions , 
      sum(searches) as searches, 
      sum(product_page) as product_page, 
      sum(nb_transaction) as nb_transaction, 
      sum(revenue) as revenue, 
    
    from {{ ref('stg_ga_sessions_daily') }}
     group by 1 , 2
     order by 1 desc 
)
   select 
         date, 
         channelgrouping, 
         unique_sessions, 
         sessions, 
         searches, 
         product_page, 
         nb_transaction as transactions, 
         revenue, 
        sum(sessions) over (partition by date) as g_sessions,
        SAFE_DIVIDE(sessions, sum(sessions) over (partition by date)) as poids_sessions, 
        sum(nb_transaction) over (partition by date) as g_transactions,
        SAFE_DIVIDE(nb_transaction, sum(nb_transaction) over (partition by date)) as poids_transactions, 
        sum(revenue) over (partition by date) as g_revenue,
        SAFE_DIVIDE(revenue, sum(revenue) over (partition by date)) as poids_revenue, 
    from ga_consolidation   
    order by 1 desc 
