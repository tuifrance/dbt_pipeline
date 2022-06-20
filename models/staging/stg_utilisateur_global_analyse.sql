{{
  config(
    materialized = 'incremental',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

with
    date_range as (
        select
            format_date('%Y%m%d', date_sub(current_date(), interval 10 day)) as start_date,
            format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) as end_date
    ), 

consolidation as (
select 
  Parse_date('%Y%m%d', date) as Date, 
  count(
    distinct CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    )
  ) as sessions, 
  COUNT(h.transaction.transactionId) as transactions, 
  sum(
    h.transaction.transactionRevenue
  ) as Revenue, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Fiche Produit - Zones de Clic' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as fich_produit, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Utilisation Moteur HP' 
    and h.eventInfo.eventAction = 'MoteurHPPackage' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as recherche_packages, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Recherche Destination - Moteur Vol' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as recherche_vols, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Option Package' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as option_packages, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Connexion - Compte Client Tunnel' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Connexion_copte, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Création - Compte Client Tunnel ' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Creation_compte, 
  count(distinct userId) as iutilisateur_identifie, 
  count(
    distinct case when channelGrouping = 'SEO' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as seo_sessions, 
  count(
    distinct case when channelGrouping = 'SEA' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as sea_sessions, 
  count(
    distinct case when channelGrouping = 'Accès direct' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as direct_sessions, 
  count(
    distinct case when channelGrouping = 'Référents' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as referents_sessions, 
  count(
    distinct case when channelGrouping = 'Affiliation' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as affiliation_sessions, 
  count(
    distinct case when channelGrouping = 'E-CRM' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as ecrm_sessions, 
  count(
    distinct case when channelGrouping = 'Display' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as display_sessions, 
  count(
    distinct case when channelGrouping = 'Social' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as social_sessions, 
  count(
    distinct case when channelGrouping = 'Paid Social' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as paid_social_sessions, 
  count(
    distinct case when channelGrouping = 'Comparateur' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as comparateur_sessions, 
  count(
    distinct case when channelGrouping = '(Other)' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as other_sessions, 
  count(
    distinct case when channelGrouping = 'Google not provided' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as not_provided_sessions, 
  sum(
    case when channelGrouping = 'SEO' then h.transaction.transactionRevenue end
  ) as seo_revenue, 
  sum(
    case when channelGrouping = 'SEA' then h.transaction.transactionRevenue end
  ) as sea_revenue, 
  sum(
    case when channelGrouping = 'Accès direct' then h.transaction.transactionRevenue end
  ) as direct_revenue, 
  sum(
    case when channelGrouping = 'Référents' then h.transaction.transactionRevenue end
  ) as referent_revenue, 
  sum(
    case when channelGrouping = 'Affiliation' then h.transaction.transactionRevenue end
  ) as affiliation_revenue, 
  sum(
    case when channelGrouping = 'E-CRM' then h.transaction.transactionRevenue end
  ) as ecrm_revenue, 
  sum(
    case when channelGrouping = 'Display' then h.transaction.transactionRevenue end
  ) as display_revenue, 
  sum(
    case when channelGrouping = 'Social' then h.transaction.transactionRevenue end
  ) as social_revenue, 
  sum(
    case when channelGrouping = 'Paid Social' then h.transaction.transactionRevenue end
  ) as paid_social_revenue, 
  sum(
    case when channelGrouping = 'Comparateur' then h.transaction.transactionRevenue end
  ) as comparateur_revenue, 
  sum(
    case when channelGrouping = '(Other)' then h.transaction.transactionRevenue end
  ) as other_revenue, 
  sum(
    case when channelGrouping = 'Google not provided' then h.transaction.transactionRevenue end
  ) as not_provided_revenue, 
  count(
    distinct case when channelGrouping = 'SEO' then h.transaction.transactionId end
  ) as seo_transaction, 
  count(
    distinct case when channelGrouping = 'SEA' then h.transaction.transactionId end
  ) as sea_transaction, 
  count(
    distinct case when channelGrouping = 'Accès direct' then h.transaction.transactionId end
  ) as direct_transaction, 
  count(
    distinct case when channelGrouping = 'Référents' then h.transaction.transactionId end
  ) as referents_transaction, 
  count(
    distinct case when channelGrouping = 'Affiliation' then h.transaction.transactionId end
  ) as affiliation_transaction, 
  count(
    distinct case when channelGrouping = 'E-CRM' then h.transaction.transactionId end
  ) as ecrm_transaction, 
  count(
    distinct case when channelGrouping = 'Display' then h.transaction.transactionId end
  ) as display_transaction, 
  count(
    distinct case when channelGrouping = 'Social' then h.transaction.transactionId end
  ) as social_transaction, 
  count(
    distinct case when channelGrouping = 'Paid Social' then h.transaction.transactionId end
  ) as paid_social_transaction, 
  count(
    distinct case when channelGrouping = 'Comparateur' then h.transaction.transactionId end
  ) as comparateur_transaction, 
  count(
    distinct case when channelGrouping = '(Other)' then h.transaction.transactionId end
  ) as other_transaction, 
  count(
    distinct case when channelGrouping = 'Google not provided' then h.transaction.transactionId end
  ) as not_provided_transaction 
From 
  {{ source('ga_tui_fr', 'ga_sessions_*') }} as ga, 
  date_range, 
  Unnest (ga.hits) as h 
where 
  _table_suffix between start_date 
  and end_date 
group by 
  1)


select * from consolidation
{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}
order by date desc 

