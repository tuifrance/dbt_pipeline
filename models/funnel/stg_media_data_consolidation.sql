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

SELECT
  date, 
  Data_Source_type,
  Campaign,
  case when data_source_type = 'doubleclicksearch' and 
    regexp_contains(lower(Account__DoubleClick_Search),'brand|hotel') then 'SEA Brand & Hotel'
    when data_source_type = 'facebookads' and regexp_contains(lower(campaign), 'remarketing')
        then 'Retargeting Social'
    when data_source_type = 'facebookads' and lower(campaign) not like '%remarketing%'
        then 'Paid Social'
        when data_source_type='criteo' then 'Retargeting Display'
        when data_source_type='tradetracker_api' then 'Affiliation'
     else 'SEA Generic' end channel_grouping,
  Account__DoubleClick_Search,
  Media_type,
  sum(Cost) as cost
from {{ ref("stg_funnel_global_data") }}
group by 1,2,3,4,5,6 