{{ config(materialized = 'table') }} with date_range as (
  select 
    '20210101' as start_date, 
    format_date(
      '%Y%m%d', 
      date_sub(
        current_date(), 
        interval 1 day
      )
    ) as end_date
) 
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
  {{ source('ga_tui_fr', 'ga_sessions_*') }}, 
  date_range, 
  Unnest (hits) as h, 
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
  7