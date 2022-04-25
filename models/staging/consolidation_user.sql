{{ config(materialized='table') }}


-- récupérer les top destination
With data1 as (select ID_EMAIL_MD5, Destination,count(Destination) as nb
from   {{ source('bq_data', 'datamart_V_032022') }}
group by ID_EMAIL_MD5, Destination),

rang as (Select ID_EMAIL_MD5, Destination, nb,ROW_NUMBER() OVER(PARTITION BY ID_EMAIL_MD5 ORDER BY nb DESC) as row_number
FROM data1
where ID_EMAIL_MD5 is not null 
order by ID_EMAIL_MD5),

top_destination as( select ID_EMAIl_MD5, top_destination_1,top_destination_2,top_destination_3
from (
    select ID_EMAIL_MD5,
case when row_number=1 then Destination end as top_destination_1,
case when row_number=2 then Destination end as top_destination_2,
case when row_number=3 then Destination end as top_destination_3
from rang)),

-- récupérer les top canal
data2 as (select ID_EMAIL_MD5, CanalRegroupe,count(CanalRegroupe) as nb
from   {{ source('bq_data', 'datamart_V_032022') }}
group by ID_EMAIL_MD5, CanalRegroupe),

rang1 as (Select ID_EMAIL_MD5, CanalRegroupe, nb,ROW_NUMBER() OVER(PARTITION BY ID_EMAIL_MD5 ORDER BY nb DESC) as row_number
FROM data2
where ID_EMAIL_MD5 is not null 
order by ID_EMAIL_MD5),

top_canal as( select ID_EMAIl_MD5, top_canal_1,top_canal_2,top_canal_3
from (
    select ID_EMAIL_MD5,
case when row_number=1 then CanalRegroupe end as top_canal_1,
case when row_number=2 then CanalRegroupe end as top_canal_2,
case when row_number=3 then CanalRegroupe end as top_canal_3
from rang1)),

data as (

SELECT ID_EMAIL_MD5,count( DISTINCT NumeroDossier ) as total_dossier,
count(  case when  statutReservation = 'Ferme' then   NumeroDossier end ) as total_dossier_ferme,
count( DISTINCT case when  statutReservation = ' Option' or  statutReservation = 'Option annulée' then   NumeroDossier end ) as total_option,
count( DISTINCT case when  statutReservation = 'Option annulée' then   NumeroDossier end ) as total_option_annule,
round(sum(safe_cast(CaBrut as FLOAT64)),2) as total_CA,
count(distinct Destination) as total_destination,
round(AVG(DATE_DIFF(cast(DateRetour as Date), cast(DateDepart as Date), day)),2) as moy_dure_sejour,  
round(sum(DATE_DIFF(cast(DateRetour as Date), cast(DateDepart as Date), day)),2) as total_duree_sejour,
count( distinct TypeProduit) as total_produit, 
count( DISTINCT case when  TypeProduit = 'Sejour Balneaire' then   NumeroDossier end ) as  Sejour_Balneaire,
count( DISTINCT case when  TypeProduit = 'Circuit' then   NumeroDossier end ) as  Circuit,
count( DISTINCT case when  TypeProduit = 'Vols secs' then   NumeroDossier end ) as  Vols_secs,
count( DISTINCT case when  TypeProduit = 'Sejour_Neige' then   NumeroDossier end ) as  Sejour_Neige,
count( DISTINCT case when  TypeProduit = 'Sejour Ville' then   NumeroDossier end ) as  Sejour_Ville,
count( DISTINCT case when  TypeProduit = 'Sejour Nature' then   NumeroDossier end ) as  Sejour_Nature,
count( DISTINCT case when  TypeProduit = 'Autotour' then   NumeroDossier end ) as  Autotour,
count( DISTINCT case when  TypeProduit = 'Croisiere' then   NumeroDossier end ) as   Croisiere,
count( DISTINCT case when  statutReservation = 'ferme' then   ID_EMAIL_MD5 end ) as nbr_achat_different,
min(DateReservation) as date_premiere_achat,
max(DateReservation) as date_dermiere_achat,
DATE_DIFF(current_date(),max(cast(DateReservation as Date)), DAY) AS  recence,
DATE_DIFF(current_date(),  min(cast(DateReservation as Date)), DAY) AS  anciennete,
round(AVG(case when  statutReservation = 'ferme' then   NbrClients end ),2) as moy_clients,
case when sum(NbrEnfants) > 0 then 1 else 0 end  as avec_sans_enfant,
case when count(case when Destination='France' then 1 else 0 end ) >1 then 1 else 0 end as sejour_france,
round(AVG(DATE_DIFF(cast(DateDepart as Date),cast(DateReservation as Date), day)),2) as delai_depart,
round(SAFE_DIVIDE(sum(safe_cast(CaBrut as FLOAT64)) , count(distinct  NumeroDossier)),2) as panier_moy,
count(distinct case when Extract(MONTH from CAST (DateDepart AS DATE)) in (1,2) then NumeroDossier end) as dossier_Hiver,
count(distinct case when Extract(MONTH from CAST (DateDepart AS DATE)) in (6,7) then NumeroDossier end) as dossier_Ete,
max(safe_cast(CaBrut as FLOAT64)) as max_depense,
min(safe_cast(CaBrut as FLOAT64)) as min_depense
FROM {{ source('bq_data', 'datamart_V_032022') }}
where ID_EMAIL_MD5 is not null  and DateRetour >= DateDepart
group by ID_EMAIL_MD5)

select data.ID_EMAIl_MD5,
total_dossier,
total_dossier ferme,
total_option,
total_option_annule,
total_CA,
total_destination,
moy_dure_sejour,
total_produit,
Sejour_Balneaire,
Circuit,
Vols_secs,
Sejour_Neige,
Sejour_Ville,
Sejour_Nature,
Autotour,
Croisiere,
nbr_achat_different,
date_premiere_achat,
date_dermiere_achat,
recence,
anciennete,
moy_clients,
avec_sans_enfant,
top_destination_1,
top_destination_2,
top_destination_3,
top_canal_1,
top_canal_2,
top_canal_3,
sejour_france,
delai_depart,
panier_moy,
dossier_Hiver,
dossier_Ete,
max_depense,
min_depense
from data 
left join top_destination as t1 on data.ID_EMAIl_MD5=t1.ID_EMAIl_MD5
left join top_canal as t2 on data.ID_EMAIl_MD5=t2.ID_EMAIl_MD5
order by data.ID_EMAIl_MD5