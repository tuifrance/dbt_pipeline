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

consolidation as
(select Parse_date('%Y%m%d',date) as Date, 
count(CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))) as sessions,
count(distinct fullVisitorId) as utilisateurs,

count(h.transaction.transactionid) as nb_transaction,
count(distinct case when trafficSource.source = 'google' then  CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))  end ) as traffic_google,
count(distinct case when trafficSource.source = 'ecrm' then  CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))  end ) as traffic_ecrm,
count(distinct case when trafficSource.source = '(direct)' then  CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))  end ) as traffic_direct,
count(distinct case when trafficSource.source = ' CRITEO' then  CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))  end ) as traffic_CRITEO,
count(distinct case when trafficSource.source = 'bing' then  CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end ) as traffic_bing,
count(distinct case when trafficSource.source = 'facebook'  then  CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))  end ) as traffic_facebook,

count(distinct case when channelGrouping= 'SEO' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end ) as SEO_sessions,
count(distinct case when channelGrouping= 'SEA' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end ) as SEA_sessions,
count(distinct case when channelGrouping= 'Accès direct' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end ) as direct_sessions,
count(distinct case when channelGrouping= 'Référents' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end ) as Referents_sessions,
count(distinct case when channelGrouping= 'Affiliation' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end ) as Affiliation_sessions,
count(distinct case when channelGrouping= 'E-CRM' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end ) as ECRM_sessions,
count(distinct case when channelGrouping= 'Display' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end ) as  Display_sessions,
count(distinct case when channelGrouping= 'Social' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end ) as Social_sessions,
count(distinct case when channelGrouping= 'Paid Social' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end ) as Paid_Social_sessions,
count(distinct case when channelGrouping= 'Comparateur' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end ) as Comparateur_sessions,
count(distinct case when channelGrouping= '(Other)' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end ) as Other_sessions,
count(distinct case when channelGrouping= 'Google not provided' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end ) as google_no_provided_sessions,

sum(distinct case when channelGrouping= 'SEO' then (h.transaction.transactionRevenue)/1000000 end ) as SEO_revenue,
sum(distinct case when channelGrouping= 'SEA' then (h.transaction.transactionRevenue)/1000000  end ) as SEA_revenue,
sum(distinct case when channelGrouping= 'Accès direct' then (h.transaction.transactionRevenue)/1000000  end ) as direct_revenue,
sum(distinct case when channelGrouping= 'Référents' then (h.transaction.transactionRevenue)/1000000  end ) as Referents_revenue,
sum(distinct case when channelGrouping= 'Affiliation' then (h.transaction.transactionRevenue)/1000000  end ) as Affiliation_revenue,
sum(distinct case when channelGrouping= 'E-CRM' then (h.transaction.transactionRevenue)/1000000  end ) as ECRM_revenue,
sum(distinct case when channelGrouping= 'Display' then (h.transaction.transactionRevenue)/1000000  end ) as Display_revenue,
sum(distinct case when channelGrouping= 'Social' then (h.transaction.transactionRevenue)/1000000  end ) as Social_revenue,
sum(distinct case when channelGrouping= 'Paid Social' then (h.transaction.transactionRevenue)/1000000  end ) as Paid_Social_revenue,
sum(distinct case when channelGrouping= 'Comparateur' then (h.transaction.transactionRevenue)/1000000  end ) as Comparateur_revenue,
sum(distinct case when channelGrouping= '(Other)' then (h.transaction.transactionRevenue)/1000000  end ) as Other_revenue,
sum(distinct case when channelGrouping= 'Google not provided' then (h.transaction.transactionRevenue)/1000000  end ) asgoogle_no_provided_revenue,

count(distinct case when channelGrouping= 'SEO' then h.transaction.transactionId end ) as SEO_transaction,
count(distinct case when channelGrouping= 'SEA' then h.transaction.transactionId end ) as SEA_transaction,
count(distinct case when channelGrouping= 'Accès direct' then h.transaction.transactionId end ) as direct_transaction,
count(distinct case when channelGrouping= 'Référents' then h.transaction.transactionId end ) as Referents_transaction,
count(distinct case when channelGrouping= 'Affiliation' then h.transaction.transactionId end ) as Affiliation_transaction,
count(distinct case when channelGrouping= 'E-CRM' then h.transaction.transactionId end ) as ECRM_transaction,
count(distinct case when channelGrouping= 'Display' then h.transaction.transactionId end ) as  Display_transaction,
count(distinct case when channelGrouping= 'Social' then h.transaction.transactionId end ) as Social_transaction,
count(distinct case when channelGrouping= 'Paid Social' then h.transaction.transactionId end ) as Paid_Social_transaction,
count(distinct case when channelGrouping= 'Comparateur' then h.transaction.transactionId end ) as Comparateur_transaction,
count(distinct case when channelGrouping= '(Other)' then h.transaction.transactionId end ) as Other_transaction,
count(distinct case when channelGrouping= 'Google not provided' then h.transaction.transactionId end ) as google_no_provided_transaction,

from {{ source('ga_tui_fr', 'ga_sessions_*') }} as ga, date_range, unnest(ga.hits) as h
where _table_suffix between start_date and end_date
group by 1
)


select * from consolidation
{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}
order by date desc