{{
  config(
    materialized = 'incremental',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}
with
date_range as (
    select
        format_date('%Y%m%d', date_sub(current_date(), interval 10 day))
            as start_date,
        format_date('%Y%m%d', date_sub(current_date(), interval 1 day))
            as end_date
),

consolidation as (
    select
        parse_date('%Y%m%d', date) as date,
        count(concat(fullvisitorid, cast(visitstarttime as STRING)))
            as sessions,
        count(distinct fullvisitorid) as utilisateurs,

        count(h.transaction.transactionid) as nb_transaction,
        count(
            distinct case
                when
                    trafficsource.source = 'google'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as traffic_google,
        count(
            distinct case
                when
                    trafficsource.source = 'ecrm'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as traffic_ecrm,
        count(
            distinct case
                when
                    trafficsource.source = '(direct)'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as traffic_direct,
        count(
            distinct case
                when
                    trafficsource.source = ' CRITEO'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as traffic_criteo,
        count(
            distinct case
                when
                    trafficsource.source = 'bing'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as traffic_bing,
        count(
            distinct case
                when
                    trafficsource.source = 'facebook'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as traffic_facebook,

        count(
            distinct case
                when
                    channelgrouping = 'SEO'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as seo_sessions,
        count(
            distinct case
                when
                    channelgrouping = 'SEA'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as sea_sessions,
        count(
            distinct case
                when
                    channelgrouping = 'Accès direct'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as direct_sessions,
        count(
            distinct case
                when
                    channelgrouping = 'Référents'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as referents_sessions,
        count(
            distinct case
                when
                    channelgrouping = 'Affiliation'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as affiliation_sessions,
        count(
            distinct case
                when
                    channelgrouping = 'E-CRM'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as ecrm_sessions,
        count(
            distinct case
                when
                    channelgrouping = 'Display'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as display_sessions,
        count(
            distinct case
                when
                    channelgrouping = 'Social'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as social_sessions,
        count(
            distinct case
                when
                    channelgrouping = 'Paid Social'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as paid_social_sessions,
        count(
            distinct case
                when
                    channelgrouping = 'Comparateur'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as comparateur_sessions,
        count(
            distinct case
                when
                    channelgrouping = '(Other)'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as other_sessions,
        count(
            distinct case
                when
                    channelgrouping = 'Google not provided'
                    then concat(fullvisitorid, cast(visitstarttime as STRING))
            end
        ) as google_no_provided_sessions,

        sum(
            distinct case
                when
                    channelgrouping = 'SEO'
                    then (h.transaction.transactionrevenue) / 1000000
            end
        ) as seo_revenue,
        sum(
            distinct case
                when
                    channelgrouping = 'SEA'
                    then (h.transaction.transactionrevenue) / 1000000
            end
        ) as sea_revenue,
        sum(
            distinct case
                when
                    channelgrouping = 'Accès direct'
                    then (h.transaction.transactionrevenue) / 1000000
            end
        ) as direct_revenue,
        sum(
            distinct case
                when
                    channelgrouping = 'Référents'
                    then (h.transaction.transactionrevenue) / 1000000
            end
        ) as referents_revenue,
        sum(
            distinct case
                when
                    channelgrouping = 'Affiliation'
                    then (h.transaction.transactionrevenue) / 1000000
            end
        ) as affiliation_revenue,
        sum(
            distinct case
                when
                    channelgrouping = 'E-CRM'
                    then (h.transaction.transactionrevenue) / 1000000
            end
        ) as ecrm_revenue,
        sum(
            distinct case
                when
                    channelgrouping = 'Display'
                    then (h.transaction.transactionrevenue) / 1000000
            end
        ) as display_revenue,
        sum(
            distinct case
                when
                    channelgrouping = 'Social'
                    then (h.transaction.transactionrevenue) / 1000000
            end
        ) as social_revenue,
        sum(
            distinct case
                when
                    channelgrouping = 'Paid Social'
                    then (h.transaction.transactionrevenue) / 1000000
            end
        ) as paid_social_revenue,
        sum(
            distinct case
                when
                    channelgrouping = 'Comparateur'
                    then (h.transaction.transactionrevenue) / 1000000
            end
        ) as comparateur_revenue,
        sum(
            distinct case
                when
                    channelgrouping = '(Other)'
                    then (h.transaction.transactionrevenue) / 1000000
            end
        ) as other_revenue,
        sum(
            distinct case
                when
                    channelgrouping = 'Google not provided'
                    then (h.transaction.transactionrevenue) / 1000000
            end
        ) as asgoogle_no_provided_revenue,

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
                when
                    channelgrouping = 'Accès direct'
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
                when
                    channelgrouping = 'Affiliation'
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
                when
                    channelgrouping = 'Paid Social'
                    then h.transaction.transactionid
            end
        ) as paid_social_transaction,
        count(
            distinct case
                when
                    channelgrouping = 'Comparateur'
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
                when
                    channelgrouping = 'Google not provided'
                    then h.transaction.transactionid
            end
        ) as google_no_provided_transaction

    from {{ source('ga_tui_fr', 'ga_sessions_*') }}, date_range,
        unnest(ga.hits) as h
    where _table_suffix between start_date and end_date
    group by 1
)


select * from consolidation
{% if is_incremental() %}
    where date > (select max(date) from {{ this }})
{% endif %}
order by date desc
