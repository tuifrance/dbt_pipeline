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
        format_date('%Y%m%d', date_sub(current_date(), interval 1 day))
            as end_date
),

consolidation as (
    select
        parse_date('%Y%m%d', date) as date,
        count(
            distinct concat(fullvisitorid, cast(visitstarttime as string))
        ) as sessions,
        count(h.transaction.transactionid) as transactions,
        sum(h.transaction.transactionrevenue) as revenue,
        count(
            distinct case
                when h.eventinfo.eventcategory = 'Fiche Produit - Zones de Clic'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as fich_produit,
        count(
            distinct case
                when
                    h.eventinfo.eventcategory = 'Utilisation Moteur HP'
                    and h.eventinfo.eventaction = 'MoteurHPPackage'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as recherche_packages,
        count(
            distinct case
                when
                    h.eventinfo.eventcategory
                    = 'Recherche Destination - Moteur Vol'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as recherche_vols,
        count(
            distinct case
                when h.eventinfo.eventcategory = 'Option Package'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as option_packages,
        count(
            distinct case
                when
                    h.eventinfo.eventcategory
                    = 'Connexion - Compte Client Tunnel'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as connexion_copte,
        count(
            distinct case
                when
                    h.eventinfo.eventcategory
                    = 'Création - Compte Client Tunnel '
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as creation_compte,
        count(distinct userid) as iutilisateur_identifie,
        count(
            distinct case
                when channelgrouping = 'SEO'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as seo_sessions,
        count(
            distinct case
                when channelgrouping = 'SEA'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as sea_sessions,
        count(
            distinct case
                when channelgrouping = 'Accès direct'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as direct_sessions,
        count(
            distinct case
                when channelgrouping = 'Référents'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as referents_sessions,
        count(
            distinct case
                when channelgrouping = 'Affiliation'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as affiliation_sessions,
        count(
            distinct case
                when channelgrouping = 'E-CRM'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as ecrm_sessions,
        count(
            distinct case
                when channelgrouping = 'Display'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as display_sessions,
        count(
            distinct case
                when channelgrouping = 'Social'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as social_sessions,
        count(
            distinct case
                when channelgrouping = 'Paid Social'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as paid_social_sessions,
        count(
            distinct case
                when channelgrouping = 'Comparateur'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as comparateur_sessions,
        count(
            distinct case
                when channelgrouping = '(Other)'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as other_sessions,
        count(
            distinct case
                when channelgrouping = 'Google not provided'
                    then concat(fullvisitorid, cast(visitstarttime as string))
            end
        ) as not_provided_sessions,
        sum(
            case
                when
                    channelgrouping = 'SEO'
                    then h.transaction.transactionrevenue
            end
        ) as seo_revenue,
        sum(
            case
                when
                    channelgrouping = 'SEA'
                    then h.transaction.transactionrevenue
            end
        ) as sea_revenue,
        sum(
            case
                when channelgrouping = 'Accès direct'
                    then h.transaction.transactionrevenue
            end
        ) as direct_revenue,
        sum(
            case
                when channelgrouping = 'Référents'
                    then h.transaction.transactionrevenue
            end
        ) as referent_revenue,
        sum(
            case
                when channelgrouping = 'Affiliation'
                    then h.transaction.transactionrevenue
            end
        ) as affiliation_revenue,
        sum(
            case
                when
                    channelgrouping = 'E-CRM'
                    then h.transaction.transactionrevenue
            end
        ) as ecrm_revenue,
        sum(
            case
                when channelgrouping = 'Display'
                    then h.transaction.transactionrevenue
            end
        ) as display_revenue,
        sum(
            case
                when channelgrouping = 'Social'
                    then h.transaction.transactionrevenue
            end
        ) as social_revenue,
        sum(
            case
                when channelgrouping = 'Paid Social'
                    then h.transaction.transactionrevenue
            end
        ) as paid_social_revenue,
        sum(
            case
                when channelgrouping = 'Comparateur'
                    then h.transaction.transactionrevenue
            end
        ) as comparateur_revenue,
        sum(
            case
                when channelgrouping = '(Other)'
                    then h.transaction.transactionrevenue
            end
        ) as other_revenue,
        sum(
            case
                when channelgrouping = 'Google not provided'
                    then h.transaction.transactionrevenue
            end
        ) as not_provided_revenue,
        count(
            distinct case
                when channelgrouping = 'SEO' then h.transaction.transactionid
            end
        ) as seo_transaction,
        count(
            distinct case
                when channelgrouping = 'SEA' then h.transaction.transactionid
            end
        ) as sea_transaction,
        count(
            distinct case
                when channelgrouping = 'Accès direct'
                    then h.transaction.transactionid
            end
        ) as direct_transaction,
        count(
            distinct case
                when
                    channelgrouping = 'Référents'
                    then h.transaction.transactionid
            end
        ) as referents_transaction,
        count(
            distinct case
                when channelgrouping = 'Affiliation'
                    then h.transaction.transactionid
            end
        ) as affiliation_transaction,
        count(
            distinct case
                when channelgrouping = 'E-CRM' then h.transaction.transactionid
            end
        ) as ecrm_transaction,
        count(
            distinct case
                when
                    channelgrouping = 'Display'
                    then h.transaction.transactionid
            end
        ) as display_transaction,
        count(
            distinct case
                when channelgrouping = 'Social' then h.transaction.transactionid
            end
        ) as social_transaction,
        count(
            distinct case
                when channelgrouping = 'Paid Social'
                    then h.transaction.transactionid
            end
        ) as paid_social_transaction,
        count(
            distinct case
                when channelgrouping = 'Comparateur'
                    then h.transaction.transactionid
            end
        ) as comparateur_transaction,
        count(
            distinct case
                when
                    channelgrouping = '(Other)'
                    then h.transaction.transactionid
            end
        ) as other_transaction,
        count(
            distinct case
                when channelgrouping = 'Google not provided'
                    then h.transaction.transactionid
            end
        ) as not_provided_transaction
    from
        {{ source("ga_tui_fr", "ga_sessions_*") }},
        date_range,
        unnest(ga.hits) as h
    where _table_suffix between start_date and end_date
    group by 1
)


select *
from consolidation
{% if is_incremental() %}
    where date > (select max(date) from {{ this }})
{% endif %}
order by date desc
