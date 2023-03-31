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
            '20210101' as start_date,
            -- format_date('%Y%m%d', date_sub(current_date(), interval 10 day)) as start_date,
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
                when channelgrouping = 'Comparateur' or lower(trafficSource.medium) like '%comparateur%'
                then 'Comparateur'
                when channelgrouping = 'Affiliation' or trafficsource.source = 'affiliation' and lower(trafficsource.source) != 'eperflex' or lower(trafficSource.medium) like '%mailing%'
                then 'Affiliation' 
                when trafficsource.medium = 'retargeting' and lower(trafficsource.source) = 'criteo' or lower(trafficsource.source) = 'eperflex' or lower(trafficsource.source)='salecycle'
                then 'Retargeting Display'
                when lower(trafficsource.source) like '%facebookads%' and trafficsource.medium in ('retargeting', 'Retargeting')
                then 'Retargeting Social'
                when lower(trafficsource.source) like '%facebookads%'
                then 'Paid Social'                
                when lower(trafficsource.medium) = 'cpm' or lower(trafficsource.campaign) like '%branding%' or lower(trafficsource.medium) like '%branding%'
                then 'Display Branding'
                when channelgrouping = 'Social' and trafficsource.medium not in ('retargeting', 'Retargeting') or lower(trafficsource.source) like '%facebook%'
                then 'Social'
                when channelgrouping in ('Accès direct') and trafficsource.source != 'qwant.com'
                then 'Direct'
                when channelgrouping in ('Référents') and trafficsource.source != 'qwant.com'
                 then 'Référents'
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
            count(distinct concat(fullvisitorid, cast(visitstarttime as string))) as users,
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