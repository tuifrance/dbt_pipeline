{{ config(materialized='table') }}

select 
  cast(substr(cast(Mois_de_Reservation as string),0,10) as date) as mois, 
  round(sum(CA_Brut),2) as online_revenue,
  sum(Nb_Cli_ts_Dossier_Finance) as online_pax, 
  round(sum(CA_Brut)/count (distinct Numero_Dossier) ,2) as average_selling_price
from
  {{ source('cdg', 'new_cdg_report') }} 
group by 1
order by 1   




  