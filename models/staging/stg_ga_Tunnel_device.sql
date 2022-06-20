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
  device.deviceCategory as device, 
  channelGrouping, 
  count(
    distinct CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    )
  ) as sessions, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Utilisation Moteur HP' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as searches, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Liste resultats' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as result_pages, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Ecommerce' 
    and h.eventInfo.eventAction = 'Product Click' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as product_clicks, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Code Produit - Fiche Produit' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as product_view, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Fiche Produit - Zones de Clic' 
    and h.eventInfo.eventAction = 'Voir les tarifs' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as product_click_price, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Ecommerce' 
    and h.eventInfo.eventAction = 'Add to cart' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as product_addtocart, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step2' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as step_two, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step2-update tuiCareFoundation' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as step_two_tuiCareFoundation, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step3Buyer' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as step_three_buyer, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Connexion - Compte Client Tunnel' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as account_login, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Création - Compte Client Tunnel' 
    and h.eventInfo.eventAction = 'track_event' 
    and h.eventInfo.eventLabel is not null then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as account_signup, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Création - Compte Client Tunnel' 
    and h.eventInfo.eventAction = 'track_event' 
    and h.eventInfo.eventLabel is null then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as account_guest, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step4' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step4, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step2ClientAccount' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step2ClientAccount, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step2-update xbags' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step2_update_xbags, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step1-Error' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step1_Error, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step2-Update room' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step2_Update_room, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step3Passengers' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step3Passengers, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step2-update promocode' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step2_update_promocode, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step4-update insurance' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step4_update_insurance, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step4-Error' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step4_Error, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step5_ConfirmationVente-Accompte25%-CB' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step5_ConfirmationVente_25, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step5_ConfirmationVente_100%_CB' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step5_ConfirmationVente_100, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step4-update promocode' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step4_update_promocode, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step5_ConfirmationOption' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step5_ConfirmationOption, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step2-update tuiCareFoundation' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step2_update_tuiCareFoundation, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step5_ConfirmationVente_3X_CB' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step5_ConfirmationVente_3X_CB, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step2-Update mealPlan' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step2_Update_mealPlan, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'Tunnel' 
    and h.contentGroup.contentGroup2 = 'Step5_ConfirmationVente_ANCV' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step5_ConfirmationVente_ANCV, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'TunnelStep1' 
    and h.contentGroup.contentGroup2 = 'Step1-Error' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step1_erreur, 
  count(
    distinct case when h.contentGroup.contentGroup1 = 'TunnelStep1' 
    and h.contentGroup.contentGroup2 = 'Step4-Error' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step4_erreur, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Option Package' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as option_package, 
  count(h.transaction.transactionId) as nb_transaction, 
  sum(
    h.transaction.transactionRevenue
  )/ 1000000 as revenue, 
  Round(
    SAFE_DIVIDE(
      sum(
        h.transaction.transactionRevenue
      )/ 1000000, 
      count(h.transaction.transactionId)
    ), 
    2
  ) as pan_moy, 
from 
  {{ source('ga_tui_fr', 'ga_sessions_*') }} as ga, 
  date_range, 
  unnest (ga.hits) as h 
where 
  _table_suffix between start_date 
  and end_date 
group by 
  1, 
  2, 
  3)

  select * from consolidation
{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}
order by date desc 

