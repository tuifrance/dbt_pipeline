{{ config(materialized='table') }}

With data as (
select  CAST (DateDepart AS DATE) as date ,
count( distinct NumeroDossier) AS total_dossiers,
count( DISTINCT case when  ID_EMAIL_MD5 is null then   NumeroDossier end ) as clients_non_identifies, 
count( DISTINCT case when  ID_EMAIL_MD5 is not null  then   NumeroDossier end ) as clients_identifies,

count(case when Destination='ESPAGNE' then NumeroDossier end) as Top_1_Espagne,
count(case when Destination='CRECE' then NumeroDossier end) as Top_2_GRECE,
count(case when Destination='ITALIE' then NumeroDossier end) as Top_3_ITALIE,
count(case when Destination='MAROC' then NumeroDossier end) as Top_4_MAROC,
count(case when Destination='FRANCE' then NumeroDossier end) as Top_5_FRANCE,
count(case when Destination='TUNISIE' then NumeroDossier end) as Top_6_TUNISIE,
count(case when Destination='REPUBLIQUE DOMINICAINE' then NumeroDossier end) as Top_7_REPUBLIQUE_DOMINICAINE,
count(case when Destination='MEXIQUE' then NumeroDossier end) as Top_8_MEXIQUE,
count(case when Destination='MARTINIQUE' then NumeroDossier end) as Top_9_MARTINIQUE,
count(case when Destination='CUBA' then NumeroDossier end) as Top_10_CUBA,
round(SAFE_DIVIDE(sum(safe_cast(CaBrut as FLOAT64)) , count(distinct  NumeroDossier)),2) as panier_moy,
round(sum( DISTINCT case when  CanalRegroupe = 'TO Prod.' then   safe_cast(CaBrut as FLOAT64) end ),2) as CA_To_prod,
round(sum( DISTINCT case when  CanalRegroupe = ' Group & Collect.' then safe_cast(CaBrut as FLOAT64) end ),2) as CA_Group_collect,
round(sum( DISTINCT case when  CanalRegroupe = 'Franchised' then   safe_cast(CaBrut as FLOAT64) end ),2) as CA_Franchised,
round(sum( DISTINCT case when  CanalRegroupe = 'Internet' then   safe_cast(CaBrut as FLOAT64) end ),2) as CA_Internet,
round(sum( DISTINCT case when  CanalRegroupe = 'Owned' then  safe_cast(CaBrut as FLOAT64) end ),2) as CA_Owned,
round(sum( DISTINCT case when  CanalRegroupe = 'Third Party' then  safe_cast(CaBrut as FLOAT64) end ) ,2)as CA_Third_party, 
round(sum( DISTINCT case when  CanalRegroupe = 'Call Center' then   safe_cast(CaBrut as FLOAT64) end ),2) as CA_Call_center,
round(sum( DISTINCT case when  CanalRegroupe = 'Non Renseigné' then safe_cast(CaBrut as FLOAT64) end ),2) as CA_Non_renseigne,
FROM  {{ source('bq_data', 'datamart_V_032022') }}
where  DateRetour >= DateDepart 
group by date
 )
select 
  date, 
  total_dossiers, 
  clients_non_identifies, 
  clients_identifies, 
  Top_1_Espagne, 
  Top_2_GRECE, 
  Top_3_ITALIE, 
  Top_4_MAROC, 
  Top_5_FRANCE, 
  Top_6_TUNISIE, 
  Top_7_REPUBLIQUE_DOMINICAINE, 
  Top_8_MEXIQUE, 
  Top_9_MARTINIQUE, 
  Top_10_CUBA, 
  panier_moy, 
  CA_To_prod, 
  CA_Group_collect, 
  CA_Franchised, 
  CA_Internet, 
  CA_Owned, 
  CA_Third_party, 
  CA_Call_center, 
  CA_Non_renseigne 
from 
  data

