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

consolidation as (
select 
  Parse_date('%Y%m%d', date) as Date, 
  device.deviceCategory as device, 
  channelGrouping, 
  -- ahouter le nom de la promotion et la position de la promotion
  SPLIT(p.promoName, 'à partir ') [OFFSET(0) ] as nom_promo, 
  p.promoPosition as position_promo, 
  h.contentGroup.contentGroup1 as page_cat, 
  h.contentGroup.contentGroup2 as page_type, 
  count(
    h.promotionActionInfo.promoIsView
  ) as Promotion_Views, 
  count(
    h.promotionActionInfo.promoIsClick
  ) AS Promotion_Clicks 
From 
  {{ source('ga_tui_fr', 'ga_sessions_*') }} as ga  , 
  date_range, 
  Unnest (ga.hits) as h, 
  Unnest (h.promotion) as p 
where 
  _table_suffix between start_date 
  and end_date 
  and p.promoId is not null 
group by 
  1, 
  2, 
  3, 
  4, 
  5, 
  6, 
  7)
  
select * from consolidation
{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}
order by date desc