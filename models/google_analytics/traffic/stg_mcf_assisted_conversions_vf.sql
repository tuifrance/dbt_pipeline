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

select
    parse_date('%Y%m%d', date) as date,
    customchannel_grouping as channel_grouping,
    sum(conversions) as conversions,
    sum(total_conversions_value) as conversions_value
from {{ ref('stg_mcf_assisted_conversions') }}
group by 1, 2
