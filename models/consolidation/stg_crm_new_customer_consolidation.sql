{{
  config(
    materialized = 'table',
    labels = {'type': 'cdg_google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

select 
        ID_EMAIL_MD5 as user_id, 
        count(distinct NumeroDossier) as nb_transactions, 
        count(distinct DateReservation) as nb_date, 
        sum(CaBrut) as total_revenue, 
        avg( cast(NbrClients as FLOAT64) ) as avg_pax
   from {{ ref('stg_crm_data_overview') }}
   group by 1