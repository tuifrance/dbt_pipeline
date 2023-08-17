{{
    config(
        materialized='table',
        labels={
            'type': 'google_analytics',
            'contains_pie': 'no',
            'category': 'production',
        },
    )
}}



with
-- 
table1 as (
    select
        date,
        clientid,
        visitnumber as purchasevisit,
        visitstarttime as purchasevisitstarttime,
        totals.transactions as transactions,
        totals.totaltransactionrevenue as revenue,
        if(
            trafficsource.istruedirect, 'Direct', channelgrouping
        ) as convertingchannel,
        (
            select max(transaction.transactionid)
            from unnest(hits) as h
            where transaction.transactionid is not null
        ) as transactionid
    from {{ source('ga_tui_fr', 'ga_sessions_*') }}
    where totals.transactions is not null
    group by
        date,
        clientid,
        purchasevisit,
        purchasevisitstarttime,
        convertingchannel,
        transactions,
        revenue,
        transactionid
),

-- 
table2 as (
    select
        clientid as clientid2,
        visitnumber,
        visitstarttime,
        last_value(
            (
                select max(transaction.transactionid)
                from unnest(hits) as h
                where transaction.transactionid is not null
            ) ignore nulls
        ) over (
            partition by fullvisitorid
            order by visitstarttime desc
            rows between unbounded preceding and current row
        ) as upcomingtransactionid,
        case
            when trafficsource.istruedirect is true
                then 'Direct'
            else channelgrouping
        end as true_channelgrouping
    from {{ source('ga_tui_fr', 'ga_sessions_*') }}
    where date between '20221001' and '20221204'
)

select
    date,
    true_channelgrouping as channel,
    case
        when purchasevisit = visitnumber then 'last click' else 'assisted click'
    end as conversiontype,
    count(distinct clientid) as users,
    sum(transactions) as transactions,
    round(sum(revenue / 1000000), 2) as revenue
from table1 as a
left join table2 as b on a.clientid = b.clientid2
where
    upcomingtransactionid = transactionid
    and purchasevisit >= visitnumber
    and purchasevisitstarttime - visitstarttime <= (30 * 24 * 60 * 60)
group by date, channel, conversiontype
