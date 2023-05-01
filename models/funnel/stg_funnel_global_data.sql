{{
    config(
        materialized='table',
        labels={
            'type': 'funnel_data',
            'contains_pie': 'no',
            'category': 'production',
        },
    )
}}

select
    date,
    data_source_type,
    account__doubleclick_search as engine,
    campaign,
    impressions,
    clicks,
    cost
from {{ source('media_data', 'funnel_data') }}
where data_source_type in ('doubleclicksearch','adwords') 

union all

select
    date, 
    data_source_type, 
    'facebookads' as engine, 
    campaign, 
    impressions, 
    clicks, 
    cost
from {{ source('media_data', 'funnel_data') }}
where data_source_type = 'facebookads'

union all 

select
    date, 
    data_source_type, 
    'criteo' as engine, 
    campaign, 
    impressions, 
    clicks, 
    cost
from {{ source('media_data', 'funnel_data') }}
where data_source_type = 'criteo'

