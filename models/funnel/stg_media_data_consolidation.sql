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
    data_source_type,
    campaign,
    CASE WHEN
        data_source_type = 'doubleclicksearch'
        AND regexp_contains(lower(account__doubleclick_search), 'brand|hotel') THEN 'SEA Brand & Hotel'
    WHEN
        data_source_type = 'facebookads'
        AND regexp_contains(lower(campaign), 'remarketing')
        THEN 'Retargeting Social'
    WHEN
        data_source_type = 'facebookads'
        AND lower(campaign) NOT LIKE '%remarketing%'
        THEN 'Paid Social'
    WHEN data_source_type = 'criteo' THEN 'Retargeting Display'
    WHEN data_source_type = 'tradetracker_api' THEN 'Affiliation'
    ELSE 'SEA Generic' END AS channel_grouping,
    account__doubleclick_search,
    media_type,
    sum(cost) AS cost
FROM {{ ref("stg_funnel_global_data") }}
GROUP BY 1, 2, 3, 4, 5, 6
