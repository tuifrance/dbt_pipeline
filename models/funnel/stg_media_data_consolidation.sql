{{
    config(
        materialized="table",
        labels={
            "type": "funnel_data",
            "contains_pie": "no",
            "category": "production",
        },
    )
}}

select
    date,
    case
        when data_source_type = 'doubleclicksearch' and engine like '%BRAND%'
        then 'SEA Brand & Hotel'
        when data_source_type = 'doubleclicksearch' and engine not like '%BRAND%'
        then 'SEA Generic'
        when data_source_type = 'adwords'
        then 'SEA Generic'
        when
            data_source_type = 'facebookads'
            and regexp_contains(lower(campaign), 'remarketing')
        then 'Retargeting Social'
        when
            data_source_type = 'facebookads'
            and lower(campaign) not like '%remarketing%'
        then 'Paid Social'
        when data_source_type='criteo' then 'Retargeting Display'
    end as channel_grouping,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    round(sum(cost), 2) as cost
from {{ ref("stg_funnel_global_data") }}
group by 1, 2
order by date desc
