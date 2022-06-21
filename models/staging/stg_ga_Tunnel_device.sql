{{
  config(
    materialized = 'incremental',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

with
    date_range as (
        select
            format_date(
                '%Y%m%d', date_sub(current_date(), interval 10 day)
            ) as start_date,
            format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) as end_date
    ),

    consolidation as (
        select
            parse_date('%Y%m%d', date) as date,
            device.devicecategory as device,
            channelgrouping,
            count(
                distinct concat(fullvisitorid, cast(visitstarttime as string))
            ) as sessions,
            count(
                distinct case
                    when h.eventinfo.eventcategory = 'Utilisation Moteur HP'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as searches,
            count(
                distinct case
                    when h.contentgroup.contentgroup1 = 'Liste resultats'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as result_pages,
            count(
                distinct case
                    when
                        h.eventinfo.eventcategory = 'Ecommerce'
                        and h.eventinfo.eventaction = 'Product Click'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as product_clicks,
            count(
                distinct case
                    when h.eventinfo.eventcategory = 'Code Produit - Fiche Produit'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as product_view,
            count(
                distinct case
                    when
                        h.eventinfo.eventcategory = 'Fiche Produit - Zones de Clic'
                        and h.eventinfo.eventaction = 'Voir les tarifs'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as product_click_price,
            count(
                distinct case
                    when
                        h.eventinfo.eventcategory = 'Ecommerce'
                        and h.eventinfo.eventaction = 'Add to cart'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as product_addtocart,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step2'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step_two,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2
                        = 'Step2-update tuiCareFoundation'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step_two_tuicarefoundation,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step3Buyer'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step_three_buyer,
            count(
                distinct case
                    when h.eventinfo.eventcategory = 'Connexion - Compte Client Tunnel'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as account_login,
            count(
                distinct case
                    when
                        h.eventinfo.eventcategory = 'Création - Compte Client Tunnel'
                        and h.eventinfo.eventaction = 'track_event'
                        and h.eventinfo.eventlabel is not null
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as account_signup,
            count(
                distinct case
                    when
                        h.eventinfo.eventcategory = 'Création - Compte Client Tunnel'
                        and h.eventinfo.eventaction = 'track_event'
                        and h.eventinfo.eventlabel is null
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as account_guest,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step4'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step4,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step2ClientAccount'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step2clientaccount,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step2-update xbags'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step2_update_xbags,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step1-Error'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step1_error,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step2-Update room'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step2_update_room,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step3Passengers'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step3passengers,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step2-update promocode'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step2_update_promocode,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step4-update insurance'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step4_update_insurance,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step4-Error'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step4_error,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2
                        = 'Step5_ConfirmationVente-Accompte25%-CB'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step5_confirmationvente_25,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2
                        = 'Step5_ConfirmationVente_100%_CB'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step5_confirmationvente_100,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step4-update promocode'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step4_update_promocode,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step5_ConfirmationOption'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step5_confirmationoption,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2
                        = 'Step2-update tuiCareFoundation'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step2_update_tuicarefoundation,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2
                        = 'Step5_ConfirmationVente_3X_CB'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step5_confirmationvente_3x_cb,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2 = 'Step2-Update mealPlan'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step2_update_mealplan,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'Tunnel'
                        and h.contentgroup.contentgroup2
                        = 'Step5_ConfirmationVente_ANCV'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step5_confirmationvente_ancv,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'TunnelStep1'
                        and h.contentgroup.contentgroup2 = 'Step1-Error'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step1_erreur,
            count(
                distinct case
                    when
                        h.contentgroup.contentgroup1 = 'TunnelStep1'
                        and h.contentgroup.contentgroup2 = 'Step4-Error'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as step4_erreur,
            count(
                distinct case
                    when h.eventinfo.eventcategory = 'Option Package'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as option_package,
            count(h.transaction.transactionid) as nb_transaction,
            sum(h.transaction.transactionrevenue) / 1000000 as revenue,
            round(
                safe_divide(
                    sum(h.transaction.transactionrevenue) / 1000000,
                    count(h.transaction.transactionid)
                ),
                2
            ) as pan_moy,
        from
            {{ source("ga_tui_fr", "ga_sessions_*") }} as ga,
            date_range,
            unnest(ga.hits) as h
        where _table_suffix between start_date and end_date
        group by 1, 2, 3
    )

select *
from consolidation
{% if is_incremental() %} where date > (select max(date) from {{ this }}) {% endif %}
order by date desc

