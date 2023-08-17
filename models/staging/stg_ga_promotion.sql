{{
  config(
    materialized = 'incremental',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

with
date_range as (
    select
        format_date(
            '%Y%m%d', date_sub(current_date(), interval 10 day)
        ) as start_date,
        format_date('%Y%m%d', date_sub(current_date(), interval 1 day))
            as end_date
),

consolidation as (
    select
        parse_date('%Y%m%d', date) as date,
        device.devicecategory as device,
        channelgrouping,
        -- ahouter le nom de la promotion et la position de la promotion
        split(p.promoname, 'à partir ')[offset(0)] as nom_promo,
        p.promoposition as position_promo,
        h.contentgroup.contentgroup1 as page_cat,
        h.contentgroup.contentgroup2 as page_type,
        count(h.promotionactioninfo.promoisview) as promotion_views,
        count(h.promotionactioninfo.promoisclick) as promotion_clicks
    from
        {{ source("ga_tui_fr", "ga_sessions_*") }},
        date_range,
        unnest(ga.hits) as h,
        unnest(h.promotion) as p
    where
        _table_suffix between start_date and end_date and p.promoid is not null
    group by 1, 2, 3, 4, 5, 6, 7
)

select *
from consolidation
{% if is_incremental() %}
    where date > (select max(date) from {{ this }})
{% endif %}
order by date desc
