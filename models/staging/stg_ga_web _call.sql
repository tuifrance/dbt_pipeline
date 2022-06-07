{{ config(materialized = 'table') }}
with date_range as (
  select 
    '20220101' as start_date, 
    format_date(
      '%Y%m%d', 
      date_sub(
        current_date(), 
        interval 1 day
      )
    ) as end_date
) 
select
  parse_date('%Y%m%d', date) as date,
  h.eventInfo.eventCategory, 
  h.eventInfo.eventAction, 
  p.productbrand as marque_produit,
  p.v2productname as product,
  trafficSource.source as source,
  trafficsource.campaign as campaign,
  trafficsource.medium as meduim,
  count(*) as total_events,
  

 FROM {{ source('ga_tui_fr', 'ga_sessions_*') }} AS GA, 
  date_range, 
  unnest(GA.hits) AS h,
  unnest(h.product) as p
  
where
  totals.visits = 1
  and h.eventInfo.eventCategory = 'iAdvize'
group by
 1,2,3,4,5,6,7,8
order by 
9 desc