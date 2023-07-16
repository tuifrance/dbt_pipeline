{{
    config(
        materialized='table',
        labels={
            'type': 'google_analytics',
            'contains_pie': 'no',
            'category': 'production',
        },
    )
}}

with
date_range as (
    select
        '20220101' as start_date,
        --format_date('%Y%m%d', date_sub(current_date(), interval 10 day)) as start_date,
        format_date('%Y%m%d', date_sub(current_date(), interval 1 day))
            as end_date
)

--     consolidation as (
select distinct
    device.devicecategory as device,
    channelgrouping,
    trafficsource.campaign,
    trafficsource.medium,
    trafficsource.source,
    trafficsource.keyword,
    trafficsource.adcontent,
    trafficsource.istruedirect,
    parse_date('%Y%m%d', date) as date,
    case
        when
            trafficsource.campaign like '%BRAND%'
            and trafficsource.medium = 'cpc'
            then 'SEA Brand & Hotel'
        when
            trafficsource.campaign not like '%BRAND%'
            and trafficsource.medium = 'cpc'
            then 'SEA Generic'
        when
            trafficsource.medium = 'organic'
            or trafficsource.medium = 'qwant.com'
            or trafficsource.medium like '%yahoo.com%'
            then 'SEO'
        when channelgrouping = 'E-CRM'
            then 'ECRM'
        when
            channelgrouping = 'Comparateur'
            or lower(trafficsource.medium) like '%comparateur%'
            then 'Comparateur'
        when
            channelgrouping = 'Affiliation'
            or trafficsource.source = 'affiliation'
            and lower(trafficsource.source) != 'eperflex'
            or lower(trafficsource.medium) like '%mailing%'
            then 'Affiliation'
        when
            trafficsource.medium = 'retargeting'
            and lower(trafficsource.source) = 'criteo'
            or lower(trafficsource.source) = 'eperflex'
            or lower(trafficsource.source) = 'salecycle'
            then 'Retargeting Display'
        when
            lower(trafficsource.source) like '%facebookads%'
            and trafficsource.medium in ('retargeting', 'Retargeting')
            then 'Retargeting Social'
        when lower(trafficsource.source) like '%facebookads%'
            then 'Paid Social'
        when
            lower(trafficsource.medium) = 'cpm'
            or lower(trafficsource.campaign) like '%branding%'
            or lower(trafficsource.medium) like '%branding%'
            then 'Display Branding'
        when
            channelgrouping = 'Social'
            and trafficsource.medium not in ('retargeting', 'Retargeting')
            or lower(trafficsource.source) like '%facebook%'
            then 'Social'
        when
            channelgrouping in ('Accès direct')
            and trafficsource.source != 'qwant.com'
            then 'Direct'
        when
            channelgrouping in ('Référents')
            and trafficsource.source != 'qwant.com'
            then 'Référents'
        else 'Autre'
    end as customchannelgrouping,


    count(distinct concat(fullvisitorid, '_', visitid)) as unique_sessions,
    count(distinct concat(fullvisitorid, cast(visitstarttime as string)))
        as sessions,
    count(
        distinct case
            when
                h.eventinfo.eventcategory = 'Utilisation Moteur HP'
                then concat(fullvisitorid, '_', visitid)
        end
    ) as searches,
    count(
        distinct case
            when
                h.eventinfo.eventcategory = 'Tri des pages resultats'
                then concat(fullvisitorid, '_', visitid)
        end
    ) as filter_searches,
    count(
        distinct case
            when
                h.eventinfo.eventcategory = 'Inscription newsletter'
                then concat(fullvisitorid, '_', visitid)
        end
    ) as signup_newsletter,
    count(
        distinct case
            when
                h.eventinfo.eventcategory = 'Code Produit - Fiche Produit'
                then concat(fullvisitorid, '_', visitid)
        end
    ) as product_page,
    count(
        distinct case
            when
                h.eventinfo.eventcategory = 'Filtre des pages resultats'
                then concat(fullvisitorid, '_', visitid)
        end
    ) as search_page,
    count(
        distinct case
            when
                h.eventinfo.eventcategory = 'Fiche Produit - Zones de Clic'
                then concat(fullvisitorid, '_', visitid)
        end
    ) as product_page_clicks,
    count(
        distinct case
            when
                totals.bounces = 1
                then concat(fullvisitorid, cast(visitstarttime as string))
        end
    ) as bounces,
    count(distinct concat(fullvisitorid, cast(visitstarttime as string)))
        as users,
    count(
        distinct case
            when
                totals.newvisits = 1
                then concat(fullvisitorid, cast(visitstarttime as string))
        end
    ) as new_users,
    count(h.transaction.transactionid) as nb_transaction,
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
    round(sum(h.transaction.transactionrevenue / 1000000), 2) as revenue

from {{ source('ga_tui_fr', 'ga_sessions_*') }} as ga, 
        date_range, 
        unnest(ga.hits) as h
        where _table_suffix between start_date and end_date and  totals.visits = 1
        group by 1, 2, 3, 4, 5, 6, 7, 8,9, 10

/*
select *
from consolidation
{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}
order by date desc
*/
