{{
  config(
    materialized = 'table',
    labels = {'type': 'crm', 'contains_pie': 'no', 'category':'production'}  
  )
}}

select 
  distinct 
  ID_EMAIL_MD5,
  lower(email) as email,
  NumeroDossier,
  --cast( (case DateFinOption in ('nan','non') then '1990-01-01' else DateFinOption end ) as date) as DateFinOption,
  cast(DateReservation as date) as DateReservation,
  RANK() OVER ( PARTITION BY ID_EMAIL_MD5 ORDER BY DateReservation asc ) as reservation_order,
  case
    when ID_EMAIL_MD5 = 'nan' then 'Unknown'
    when RANK() OVER ( PARTITION BY ID_EMAIL_MD5 ORDER BY DateReservation asc ) = 1 then 'New Customer'
    when RANK() OVER ( PARTITION BY ID_EMAIL_MD5 ORDER BY DateReservation asc ) > 1 then 'Old Customer'
    end as customer_type ,
  --cast(Date1ereConfirmation as date) as Date1ereConfirmation,
  cast(DateDepart as date) as DateDepart,
  --cast(DateRetour as date) as DateRetour,
  --cast(DateAnnulation as date) as DateAnnulation,
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
  DMAJ, 
  RANK() OVER ( PARTITION BY email ORDER BY cast(DateReservation as date) asc  ) AS numero_transaction
  from {{ source('crm', 'WS_DIGITAL_DATAMART_DOSSIER') }}
  where ID_EMAIL_MD5 is not null 
