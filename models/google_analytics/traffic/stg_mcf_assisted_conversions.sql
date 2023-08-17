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
with consolidation as (
    select
        conversion_date as date,
        source,
        medium,
        campaign_name,
        channel_grouping,
        cast(total_conversions as float64) as conversions,
        cast(total_conversions_value as float64) as total_conversions_value,
        case
            when campaign_name like '%BRAND%' and medium = 'cpc'
                then 'SEA Brand & Hotel'
            when campaign_name not like '%BRAND%' and medium = 'cpc'
                then 'SEA Generic'
            when
                medium = 'organic'
                or medium = 'qwant.com'
                or medium like '%yahoo.com%'
                then 'SEO'
            when channel_grouping = 'E-CRM' or source = 'ecrm'
                then 'ECRM'
            when channel_grouping = 'Comparateur'
                then 'Comparateur'
            when
                channel_grouping = 'Affiliation'
                or source = 'affiliation'
                or medium = 'affiliation'
                and source != 'EPERFLEX'
                then 'Affiliation'
            when
                medium = 'retargeting' and source = 'CRITEO'
                or source = 'EPERFLEX'
                then 'Retargeting Display'
            when
                source = 'Facebookads'
                and medium in ('retargeting', 'Retargeting')
                then 'Retargeting Social'
            when
                medium = 'cpm'
                or campaign_name like '%branding%'
                or medium like '%branding%'
                then 'Display Branding'
            when
                source = 'Facebookads'
                and medium not in ('retargeting', 'Retargeting')
                then 'Paid Social'
            when
                channel_grouping = 'Social'
                and medium not in ('retargeting', 'Retargeting')
                then 'Social'
            when
                channel_grouping in ('Accès Direct', 'Référents')
                and source != 'qwant.com'
                then 'Direct'
            else 'Autre'
        end as customchannel_grouping
    from {{ source('assist_conv', 'Assisted_Conversions') }}
    order by conversion_date desc
)

select *
from consolidation
{% if is_incremental() %}
    where date > (select max(date) from {{ this }})
{% endif %}
order by date desc
