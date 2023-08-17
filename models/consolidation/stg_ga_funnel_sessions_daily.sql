{{
  config(
    materialized = 'table',
    labels = {'type': 'cdg_google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

select
    ga_date,
    ga_channel,
    ga_campaign,
    ga_source,
    ga_medium,
    ga_new_users,
    ga_sessions,
    ga_bounces,
    ga_session_duration,
    ga_pageviews
        as ga_transactions,
    ga_revenue,
    case
        when
            ga_campaign like '%BRAND%'
            and ga_medium = 'cpc'
            then 'SEA Brand & Hotel'
        when
            ga_campaign not like '%BRAND%'
            and ga_medium = 'cpc'
            then 'SEA Generic'
        when
            ga_medium = 'organic'
            or ga_medium = 'qwant.com'
            or ga_medium like '%yahoo.com%'
            then 'SEO'
        when ga_channel = 'E-CRM' or regexp_contains(lower(ga_source), 'ecrm')
            then 'ECRM'
        when
            ga_channel = 'Comparateur'
            or lower(ga_medium) like '%comparateur%'
            then 'Comparateur'
        when
            ga_channel = 'Affiliation'
            or ga_source = 'affiliation'
            or lower(ga_medium) like '%affiliation%'
            and lower(ga_source) != 'eperflex'
            or lower(ga_medium) like '%mailing%'
            then 'Affiliation'
        when
            ga_medium = 'retargeting'
            and lower(ga_source) = 'criteo'
            or lower(ga_source) = 'eperflex'
            or lower(ga_source) = 'salecycle'
            then 'Retargeting Display'
        when
            lower(ga_source) like '%facebookads%'
            and ga_medium in ('retargeting', 'Retargeting')
            then 'Retargeting Social'
        when lower(ga_source) like '%facebookads%'
            then 'Paid Social'
        when
            lower(ga_medium) = 'cpm'
            or lower(ga_campaign) like '%branding%'
            or lower(ga_medium) like '%branding%'
            then 'Display Branding'
        when
            ga_channel = 'Social'
            and ga_medium not in ('retargeting', 'Retargeting')
            or lower(ga_source) like '%facebook%'
            then 'Social'
        when
            ga_channel in ('Accès direct') or lower(ga_source) like '%direct%'
            and ga_source != 'qwant.com'
            then 'Direct'
        when
            lower(ga_medium) like '%referral%'
            then 'Référents'
        when
            ga_channel in ('Référents')
            and ga_source != 'qwant.com'
            then 'Référents'
        else 'Autre'
    end as customga_channel
from {{ ref('stg_funnel_ga_data') }}
