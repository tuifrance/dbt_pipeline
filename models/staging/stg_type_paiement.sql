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
            format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) as end_date
    ),

    consolidation as (
        select
            parse_date('%Y%m%d', date) as date,
            device.devicecategory as device,
            channelgrouping,
            h.transaction.transactionid as dossier,
            (h.transaction.transactionrevenue) / 1000000 as revenue,
            (
                select x.value from unnest(h.customdimensions) as x where x.index = 72
            ) as type_paiement,


        from
            {{ source("ga_tui_fr", "ga_sessions_*") }} as ga,
            date_range,
            unnest(ga.hits) as h
        where
            _table_suffix between start_date
            and end_date
            and h.transaction.transactionid is not null
    )

select *
from consolidation
{% if is_incremental() %} where date > (select max(date) from {{ this }}) {% endif %}
order by date desc
