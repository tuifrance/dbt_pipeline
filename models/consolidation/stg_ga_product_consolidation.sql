{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

with data_fiche_produit as (
select 
  date,
  code_produit,
  concat(date,'_',code_produit) as unique_ligne_id,
  sum(consultation) as consultation,
  sum(voir_tarif) as click_voir_tarif,
  sum(ville_depart) as click_ville_depart,
  sum(duration) as click_duration,
  sum(goFunnel) as click_go_funnel
from {{ ref('stg_ga_fiche_produit') }}
group by 1,2,3
), 
data_cdg as ( 
select 
  Date_de_Reservation,
  Code_Produit as cdg_code_produit,
  concat(Date_de_Reservation,'_', Code_Produit) as unique_ligne_id,
  Produit as cdg_produit,
  Categorie_CRM_Produit as category,
  Destination_TO as destination, 
  count(distinct Numero_Dossier) as cdg_ventes,
  sum(Nb_Cli_ts_Dossier_Finance) as cdg_pax,
  sum(CA_Brut) as cdg_revenue
from 
  {{ ref('stg_cdg_overview') }}
group by 1,2,3,4,5,6   

)
select 
  Date_de_Reservation, 
  cdg_code_produit, 
  cdg_produit, 
  category, 
  destination, 
  cdg_ventes, 
  cdg_revenue, 
  code_produit, 
  consultation

  from data_cdg
  left join data_fiche_produit
  on data_cdg.unique_ligne_id = data_fiche_produit.unique_ligne_id