{{
  config(
    materialized = 'table',
    labels = {'type': 'cdg_google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

with data_ga as (
select 
         date, 
         channelgrouping, 
         unique_sessions, 
         sessions, 
         searches, 
         product_page, 
         transactions, 
         revenue, 
         g_sessions,
         poids_sessions, 
         g_transactions,
         poids_transactions, 
         g_revenue,
         poids_revenue, 
      from {{ ref('stg_ga_consolidation') }}
) , 

data_cdg as (
select 
      Date_de_Reservation , 
      ventes, 
      pax , 
      revenue
     from  {{ ref('stg_cdg_consolidation') }}
)

select 
    data_ga.date,
    data_ga.channelgrouping, 
    concat (data_ga.date, '_',data_ga.channelgrouping) as ligne_id, 
    data_ga.sessions, 
    data_ga.searches, 
    data_ga.transactions,
    data_ga.revenue as ga_revenue,  
    data_ga.g_transactions,
    data_ga.poids_transactions,
    data_ga.g_revenue,    
    data_ga.poids_revenue, 
    data_cdg.ventes, 
    data_cdg.revenue as cdg_revenue,
    data_ga.poids_transactions * data_cdg.ventes as final_ventes, 
    data_ga.poids_revenue * data_cdg.revenue as final_revenue,
 from data_ga
 left join data_cdg
 on data_ga.date = data_cdg.Date_de_Reservation






 