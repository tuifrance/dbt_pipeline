{{
  config(
    materialized = 'table',
    labels = {'type': 'crm_google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

with data_transaction as (
select 
  distinct 
  date,
  device,
  channelgrouping,
  campaign,
  medium,
  SOURCE,
  keyword,
  adContent,
  transactionid,
  type_voyage,
  user_id,
  eventcategory,
  eventaction,
  code_produit,
  code_parent_produit,
  nom_produit,
  --destination,
  ville_depart,
  ville_arrive,
  pax_total,
  pax_adult,
  pax_enfant,
  pax_bebe,
  duree_sejour,
  continent,
  type_paiement,
  prixproduit,
  revenue
 from {{ ref('stg_ga_transactions_daily') }}
), 
 
 data_crm as ( 

select 
  ID_EMAIL_MD5,
  NumeroDossier,
  statutReservation,
  TypeDossier,
  CodeAgence,
  Reseau,
  CanalRegroupe,
  Marque,
  CodeProduit,
  Produit,
  TypeProduit,
  GroupeMarketingProduit,
  Gamme,
  GammeGroupe,
  Destination,
  CodeSejour,
  NatureSejour,
  FournisseurSejour,
  GroupeMarketingConsoSejour,
  GroupeConceptSejourConso,
  ConceptSejourConso,
  VilleSejour,
  PaysSejour,
  VilleDepart,
  VilleArrivee,
  TerminalDepart,
  TerminalArrivee,
  NbrClients,
  CaBrut,
  customer_type
  from {{ ref('stg_crm_data_overview') }}
 )

select * 
 from data_transaction
 left join data_crm
 on data_transaction.transactionid = data_crm.NumeroDossier
 order by data_transaction.date desc 
