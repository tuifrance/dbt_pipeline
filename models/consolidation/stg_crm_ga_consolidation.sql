{{
  config(
    materialized = 'table',
    labels = {'type': 'cdg_google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

with data_ga as (
select 
  date,
  device,
  channelgrouping,
  campaign,
  medium,
  SOURCE,
  transactionid,
  type_voyage,
  user_id,
  eventcategory,
  eventaction,
  code_produit,
  code_parent_produit,
  nom_produit,
  destination as ga_destinatiion,
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
) , 

data_crm as ( 
 
  select 
      ID_EMAIL_MD5,
      NumeroDossier,  
      DateFinOption,
      DateReservation,
      reservation_order,
      customer_type ,
      Date1ereConfirmation,
      DateDepart,
      DateRetour,
      DateAnnulation,
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
      CaBrut, 
      DMAJ,
      numero_transaction

      from {{ ref('stg_crm_data_overview') }}
     

)

select 
   * from data_ga  
   left join data_crm
   on data_ga.transactionid = data_crm.NumeroDossier