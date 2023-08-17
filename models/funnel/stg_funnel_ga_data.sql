with source as (
    select * from {{ source('media_data', 'ga_export') }}
),

renamed as (


    select
        date as ga_date,
        default_channel_grouping___ua__google_analytics as ga_channel,
        revenue___ua__google_analytics as ga_revenue,
        transactions___ua__google_analytics as ga_transactions,
        pageviews___ua__google_analytics as ga_pageviews,
        bounces___ua__google_analytics as ga_bounces,
        session_duration___ua__google_analytics as ga_session_duration,
        sessions___ua__google_analytics as ga_sessions,
        campaign___ua__google_analytics as ga_campaign,
        medium___ua__google_analytics as ga_medium,
        source___ua__google_analytics as ga_source,
        source__medium___ua__google_analytics as ga_source_medium,
        new_users___ua__google_analytics as ga_new_users
    from
        source
)

select * from renamed
