{{
  config(
    materialized = 'table',
    labels = {'type': 'cdg', 'contains_pie': 'no', 'category':'production'}  
  )
}}

select 
      Date_de_Reservation , 
      count(distinct Numero_Dossier) as ventes, 
      sum(Nb_Cli_ts_Dossier_Finance) as pax , 
      sum(CA_Brut) as revenue
     from  {{ ref('stg_cdg_overview') }}
     group by 1 
     order by 1 desc 
