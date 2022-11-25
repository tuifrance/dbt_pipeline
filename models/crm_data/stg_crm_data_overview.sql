{{
  config(
    materialized = 'table',
    labels = {'type': 'crm', 'contains_pie': 'no', 'category':'production'}  
  )
}}
with info as (
select 
  ID_EMAIL_MD5,
  lower(email) as email,
  NumeroDossier,
  cast(DateFinOption as date) as DateFinOption,
  cast(DateReservation as date) as DateReservation,
  RANK() OVER ( PARTITION BY ID_EMAIL_MD5 ORDER BY DateReservation asc ) as reservation_order,
  case 
    when RANK() OVER ( PARTITION BY ID_EMAIL_MD5 ORDER BY DateReservation asc ) = 1 then 'New Customer'
    when RANK() OVER ( PARTITION BY ID_EMAIL_MD5 ORDER BY DateReservation asc ) > 1 then 'Old Customer'
    end as customer_type ,
  cast(Date1ereConfirmation as date) as Date1ereConfirmation,
  cast(DateDepart as date) as DateDepart,
  cast(DateRetour as date) as DateRetour,
  cast(DateAnnulation as date) as DateAnnulation,
  statutReservation,
  TypeDossier,
  CodeAgence,
  Agence,
  CodePointDeVente,
  PointDeVente,
  Reseau,
  Canal,
  CanalRegroupe,
  CanalCalcule,
  DistributionConso,
  CanalDistributionNiveau1,
  Marque,
  CodeProduit,
  Produit,
  TypeProduit,
  GroupeMarketingProduit,
  Gamme,
  GammeGroupe,
  CodeIso3destination,
  Destination,
  CodeVilleDestination,
  CodeSejour,
  NatureSejour,
  FournisseurSejour,
  GroupeMarketingConsoSejour,
  GroupeConceptSejourConso,
  ConceptSejourConso,
  VilleSejour,
  PaysSejour,
  CodeCircuit,
  Circuit,
  NatureCircuit,
  FournisseurCircuit,
  GroupeMarketingCircuit,
  PaysArriveeCircuit,
  VilleArriveeCircuit,
  MarqueCircuit,
  VilleDepart,
  VilleArrivee,
  CodeIso3PaysVilleArrivee,
  TerminalDepart,
  TerminalArrivee,
  FlagRefPart,
  FlagReductionRefPart,
  NbrClients,
  NbrAdultes,
  NbrEnfants,
  NbrBebes,
  cast(REPLACE (CaBrut, ',', '.') as FLOAT64) as  CaBrut, 
  DMAJ
  from {{ source('crm', 'QT_032_WS_DIGITAL_DATAMART_DOSSIER20221115') }}
  where ID_EMAIL_MD5 is not null 
  ),
  numero_transaction as (
  select *
  from
      (
          select distinct
              ID_EMAIL_MD5 as temp_id,
              rank() over (partition by ID_EMAIL_MD5 order by DateReservation) numero_transaction
          from {{ source('crm', 'QT_032_WS_DIGITAL_DATAMART_DOSSIER20221115') }}
      )
)
select * except (temp_id)
from info a
left join numero_transaction b on a.ID_EMAIL_MD5 = b.temp_id
order by ID_EMAIL_MD5 asc , DateReservation asc
