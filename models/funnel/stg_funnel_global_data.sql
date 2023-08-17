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

select *
from {{ source('media_data', 'funnel_data') }}
