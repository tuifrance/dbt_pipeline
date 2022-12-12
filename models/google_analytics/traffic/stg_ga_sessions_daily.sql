{{
    config(
        materialized='incremental',
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

            format_date('%Y%m%d', date_sub(current_date(), interval 10 day)) as start_date,
            format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) as end_date
    ), 
    consolidation as (
        select distinct
            parse_date('%Y%m%d', date) as date,
            device.devicecategory as device,
            case
                when trafficsource.campaign like '%BRAND%' and trafficsource.medium = 'cpc'
                then 'SEA Brand & Hotel'
                when trafficsource.campaign not like '%BRAND%' and trafficsource.medium = 'cpc'
                then 'SEA Generic'
                when trafficsource.medium = 'organic' or trafficsource.medium = 'qwant.com' or trafficsource.medium like '%yahoo.com%'
                then 'SEO'
                when channelgrouping = 'E-CRM'
                then 'ECRM'
                when channelgrouping = 'Comparateur'
                then 'Comparateur'
                when channelgrouping = 'Affiliation' or trafficsource.source = 'affiliation' and trafficsource.source != 'EPERFLEX'
                then 'Affiliation'
                when trafficsource.medium = 'retargeting' and trafficsource.source = 'CRITEO' or trafficsource.source = 'EPERFLEX'
                then 'Retargeting Display'
                when trafficsource.source = 'Facebookads' and trafficsource.medium in ('retargeting', 'Retargeting')
                then 'Retargeting Social'
                when trafficsource.medium = 'cpm' or trafficsource.campaign like '%branding%' or trafficsource.medium like '%branding%'
                then 'Display Branding'
                when trafficsource.source = 'Facebookads' and trafficsource.medium not in ('retargeting', 'Retargeting')
                then 'Paid Social'
                when channelgrouping = 'Social' and trafficsource.medium not in ('retargeting', 'Retargeting')
                then 'Social'
                when channelgrouping in ('Accès Direct', 'Référents') and trafficsource.source != 'qwant.com'
                then 'Direct'
                else 'Autre'
            end as customchannelgrouping,
            channelGrouping	, 
            trafficsource.campaign,
            trafficsource.medium,
            trafficsource.source,
            trafficSource.keyword, 
            trafficSource.adContent, 
            trafficSource.isTrueDirect, 


            count( distinct concat(fullvisitorid, '_', visitId)) as unique_sessions, 
            count(distinct concat(fullvisitorid, cast(visitstarttime as string))) as sessions,
            count( distinct case when h.eventInfo.eventCategory = 'Utilisation Moteur HP' then concat(fullvisitorid, '_', visitId) end ) as searches,
            count( distinct case when h.eventInfo.eventCategory = 'Code Produit - Fiche Produit' then concat(fullvisitorid, '_', visitId) end)  as product_page,
            count( distinct case when h.eventInfo.eventCategory = 'Filtre des pages resultats' then concat(fullvisitorid, '_', visitId) end)  as search_page,
            count( distinct case when h.eventInfo.eventCategory = 'Fiche Produit - Zones de Clic' then concat(fullvisitorid, '_', visitId) end)  as product_page_clicks,
            count(distinct case when totals.bounces = 1 then concat(fullvisitorid, cast(visitstarttime as string)) end) as bounces,
            count(distinct case when totals.newvisits = 1 then concat(fullvisitorid, cast(visitstarttime as string)) end) as new_users,
            count(h.transaction.transactionid) as nb_transaction,
            round(sum(h.transaction.transactionrevenue / 1000000), 2) as revenue

        from {{ source('ga_tui_fr', 'ga_sessions_*') }} as ga, 
        date_range, 
        unnest(ga.hits) as h
        where _table_suffix between start_date and end_date and  totals.visits = 1
        group by 1, 2, 3, 4, 5, 6, 7, 8,9, 10
    )
select *
from consolidation
{% if is_incremental() %} where date > (select max(date) from {{ this }}) {% endif %}
order by date desc