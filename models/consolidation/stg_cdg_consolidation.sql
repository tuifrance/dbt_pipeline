{{
  config(
    materialized = 'table',
    labels = {'type': 'cdg', 'contains_pie': 'no', 'category':'production'}  
  )
}}

select
    DATE_DE_RESERVATION,
    count(distinct NUMERO_DOSSIER) as VENTES,
    sum(NB_CLI_TS_DOSSIER_FINANCE) as PAX,
    sum(CA_BRUT) as REVENUE
from {{ ref('stg_cdg_overview') }}
group by 1
order by 1 desc
