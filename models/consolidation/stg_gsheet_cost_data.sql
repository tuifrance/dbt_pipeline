{{
    config(
        materialized='table'
    )
}}

with data_consolidation as (
select 
   parse_date('%Y%m%d', date) as date,
   channel, 
   case when channel = 'SEA Circuits' then 'SEA Generic' else channel end as channel_grouping,
   cast(cost as  float64) as cost
 from {{ source('cost', 'cost_data_2023') }}
)
select 
   date, 
   channel_grouping, 
   sum(cost) as cost
  from data_consolidation
  group by 1,2