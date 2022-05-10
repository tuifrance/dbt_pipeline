{{ config(materialized = 'table') }} with date_range as (
  select 
    '20210101' as start_date, 
    format_date(
      '%Y%m%d', 
      date_sub(
        current_date(), 
        interval 1 day
      )
    ) as end_date
) 
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
  ) as product_see_price, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Fiche Produit - Zones de Clic' 
    and h.eventInfo.eventAction = 'FP-calendrier' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as product_see_calendar, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Ecommerce' 
    and h.eventInfo.eventAction = 'Add to cart' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as product_addtocart, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Connexion - Compte Client Tunnel' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as product_connexion, 
  count(
    distinct case when h.eventInfo.eventCategory = 'Utilisation Moteur HP' then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as searches, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step2" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as step2, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step4" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step4, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step2ClientAccount" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step2ClientAccount, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step3Buyer" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step3Buyer, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step2-update xbags" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step2_update_xbags, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step1-Error" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step1_Error, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step2-Update room" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step2_Update_room, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step3Passengers" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step3Passengers, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step2-update promocode" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step2_update_promocode, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step4-update insurance" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step4_update_insurance, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step4-Error" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step4_Error, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step5_ConfirmationVente-Accompte25%-CB" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step5_ConfirmationVente_25, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step5_ConfirmationVente_100%_CB" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step5_ConfirmationVente_100, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step4-update promocode" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step4_update_promocode, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step5_ConfirmationOption" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step5_ConfirmationOption, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step2-update tuiCareFoundation" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step2_update_tuiCareFoundation, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step5_ConfirmationVente_3X_CB" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step5_ConfirmationVente_3X_CB, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step2-Update mealPlan" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step2_Update_mealPlan, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "Tunnel" 
    and h.contentGroup.contentGroup2 = "Step5_ConfirmationVente_ANCV" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step5_ConfirmationVente_ANCV, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "TunnelStep1" 
    and h.contentGroup.contentGroup2 = "Step1-Error" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step1_erreur, 
  count(
    distinct case when h.contentGroup.contentGroup1 = "TunnelStep1" 
    and h.contentGroup.contentGroup2 = "Step4-Error" then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as Step4_erreur, 
  count(h.transaction.transactionId) as nb_transaction, 
  sum(
    h.transaction.transactionRevenue
  ) as Revenue 
from 
  {{ source('ga_tui_fr', 'ga_sessions_*') }}, 
  date_range, 
  unnest (hits) as h 
where 
  _table_suffix between start_date 
  and end_date 
group by 
  1, 
  2, 
  3
