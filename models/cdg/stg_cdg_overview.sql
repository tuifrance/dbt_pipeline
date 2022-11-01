{{
  config(
    materialized = 'table',
    labels = {'type': 'cdg', 'contains_pie': 'no', 'category':'production'}  
  )
}}
     select 
        cast(Mois_de_Reservation as date) as Mois_de_Reservation,
        Semaine_de_Reservation,
        Red__part_aire_Y,
        cast(Date_de_Reservation as date) as Date_de_Reservation,
        Numero_Dossier,
        cast(Mois_de_Depart as date) as Mois_de_Depart, 
        cast(Date_de_Depart as date) as Date_de_Depart,
        Package__Y_N_,
        Marque,
        Groupe_Marketing_Produit,
        Ag_ce_Consolidee_Dossier_Viaxeo,
        Ag_ce_detaillee,
        Point_de_V_te,
        Code_Ville_Depart_TUSSY,
        Pays_Destination_Consolide_Finance,
        Destination_TO,
        Code_Ville_Arrivee_TUSSY,
        Code_Produit,
        Produit,
        Categorie_CRM_Produit,
        Promotion__Y_N_,
        Groupe_Duree_de_Sejour_Detail__EN_,
        Duree_de_Sejour,
        Dossier_A_Valoir__Y_N_,
        A_Valoir_Genere__Y_N_,
        Dossier_Report_Suite_Covid19__Y_N_,
        cast(Nb_Cli_ts_Dossier_Finance as FLOAT64) as Nb_Cli_ts_Dossier_Finance,
        cast(CA_Brut as FLOAT64 ) as CA_Brut

   from {{ source('cdg', 'historic_new_cdg') }} 
     order by Date_de_Reservation desc 





