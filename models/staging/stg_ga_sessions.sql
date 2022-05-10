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
  distinct parse_date('%Y%m%d', date) as date, 
  device.deviceCategory as device, 
  case when trafficSource.campaign like '%BRAND%' 
  and trafficSource.medium = 'cpc' then 'SEA Brand & Hotel' when trafficSource.campaign not like '%BRAND%' 
  and trafficSource.medium = 'cpc' then 'SEA Generic' when trafficSource.medium = 'organic' 
  OR trafficSource.medium = 'qwant.com' 
  OR trafficSource.medium like '%yahoo.com%' then 'SEO' when channelGrouping = 'E-CRM' then 'ECRM' when channelGrouping = 'Comparateur' then 'Comparateur' when channelGrouping = 'Affiliation' 
  or trafficSource.source = 'affiliation' 
  AND trafficSource.source != 'EPERFLEX' then 'Affiliation' when trafficSource.medium = 'retargeting' 
  AND trafficSource.source = 'CRITEO' 
  OR trafficSource.source = 'EPERFLEX' then 'Retargeting Display' when trafficSource.source = 'Facebookads' 
  AND trafficSource.medium in ('retargeting', 'Retargeting') then 'Retargeting Social' when trafficSource.medium = 'cpm' 
  or trafficSource.campaign like '%branding%' 
  or trafficSource.medium like '%branding%' then 'Display Branding' when trafficSource.source = 'Facebookads' 
  AND trafficSource.medium not in ('retargeting', 'Retargeting') then 'Paid Social' when channelGrouping = 'Social' 
  and trafficSource.medium not in ('retargeting', 'Retargeting') then 'Social' when channelGrouping in ('Accès Direct', 'Référents') 
  and trafficSource.source != 'qwant.com' then 'Direct' else 'Autre' end as customChannelGrouping, 
  trafficSource.campaign, 
  trafficSource.medium, 
  trafficSource.source, 
  count(
    distinct CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    )
  ) as sessions, 
  count (
    distinct case when totals.bounces = 1 then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as bounces, 
  count (
    distinct case when totals.newVisits = 1 then CONCAT(
      fullVisitorId, 
      CAST(visitStartTime AS STRING)
    ) end
  ) as new_users, 
  count(h.transaction.transactionId) as nb_transaction, 
  round(
    sum(
      h.transaction.transactionRevenue / 1000000
    ), 
    2
  ) as Revenue 
FROM 
  {{ source('ga_tui_fr', 'ga_sessions_*') }} AS GA, 
  date_range, 
  unnest(GA.hits) AS h 
where 
  _table_suffix between start_date 
  and end_date 
group by 
  1, 
  2, 
  3, 
  4, 
  5, 
  6
