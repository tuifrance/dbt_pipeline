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
  nom_produit,
  destination,
  type_voyage,
  sum(consultation) as consultation,
  sum(voir_tarif) as click_voir_tarif,
  sum(ville_depart) as click_ville_depart,
  sum(duration) as click_duration,
  sum(goFunnel) as click_go_funnel
from {{ ref('stg_ga_fiche_produit') }}
group by 1,2,3,4,5,6
), 
data_transaction as (
select
  date as ga_date,
  code_produit as ga_code_produit,
  concat (date, '_', code_produit) as unique_ligne_id ,
  count(distinct transactionid) as ventes,
  sum(revenue) as revenue
 from 
    {{ ref('stg_ga_transactions_daily') }}
group by 1,2,3

), 
data_cdg as ( 
select 
  Date_de_Reservation,
  Code_Produit as cdg_code_produit,
  concat(Date_de_Reservation,'_', Code_Produit) as unique_ligne_id,
  Produit as cdg_produit,
  Categorie_CRM_Produit,
  count(distinct Numero_Dossier) as cdg_ventes,
  sum(Nb_Cli_ts_Dossier_Finance) as cdg_pax,
  sum(CA_Brut) as cdg_revenue
from 
  {{ ref('stg_cdg_overview') }}
group by 1,2,3,4,5    

)
select 
  *  except(unique_ligne_id)
  from data_fiche_produit
  left join data_transaction
  on data_fiche_produit.unique_ligne_id = data_transaction.unique_ligne_id
  left join data_cdg
  on data_fiche_produit.unique_ligne_id = data_cdg.unique_ligne_id
  order by 1 desc 