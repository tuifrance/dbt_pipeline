{{
    config(
        materialized='table',
        labels={
            'type': 'cdg_google_analytics',
            'contains_pie': 'no',
            'category': 'production',
        },
    )
}}

with
    data_crm as (
        select
            date,
            channelgrouping,
            concat(date, '_', channelgrouping) as unique_id,
            count(distinct transactionid) as ventes_ga,
            count(distinct id_email_md5) as total_customers,
            count(
                distinct case when customer_type = 'Old Customer' then id_email_md5 end
            ) as old_customers,
            count(
                distinct case when customer_type = 'New Customer' then id_email_md5 end
            ) as new_customers
        from {{ ref('stg_crm_ga_consolidation') }}
        group by 1, 2, 3
    ),

    data_ga as (

        select
            date,
            channelgrouping,
            concat(date, '_', channelgrouping) as unique_id,
            sessions,
            transactions,
            final_revenue as revenue_cdg,
            final_ventes as transactions_cdg,
        from {{ ref('stg_ga_cdg_consolidation') }}

    ),

    media_data as (

        select
            date,
            channel_grouping,
            concat(date, '_', channel_grouping) as unique_id,
            impressions,
            clicks,
            cost
        from {{ ref('stg_media_data_consolidation') }}
    )

select
    data_ga.date,
    data_ga.channelgrouping,
    data_ga.sessions,
    data_ga.transactions,
    round(data_ga.revenue_cdg,2) as revenue_cdg ,
    round(data_ga.transactions_cdg,2) as transactions_cdg ,
    media_data.impressions,
    media_data.clicks,
    media_data.cost,
    data_crm.total_customers,
    data_crm.old_customers,
    data_crm.new_customers
from data_ga
left join data_crm on data_ga.unique_id = data_crm.unique_id
left join media_data on data_ga.unique_id = media_data.unique_id
order by data_ga.date desc
