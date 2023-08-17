{{
  config(
    materialized = 'table',
    labels = {'type': 'cdg_google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

select
    ID_EMAIL_MD5 as USER_ID,
    count(distinct NUMERODOSSIER) as NB_TRANSACTIONS,
    count(distinct DATERESERVATION) as NB_DATE,
    sum(CABRUT) as TOTAL_REVENUE,
    avg(cast(NBRCLIENTS as FLOAT64)) as AVG_PAX
from {{ ref('stg_crm_data_overview') }}
group by 1
