{{ config(materialized='table') }}


with date_range as (select '20220101' as start_date,
format_date('%Y%m%d',date_sub(current_date(),interval 1 day)) as end_date
)
select 
        Parse_date('%Y%m%d',date) as Date,
        device.deviceCategory as type_appareil,	
		channelGrouping as channel, 			
		(SELECT x.value FROM UNNEST(h.customDimensions) x WHERE x.index = 41) as destination,
        (SELECT x.value FROM UNNEST(h.customDimensions) x WHERE x.index = 33) as ville_depart,
        (SELECT x.value FROM UNNEST(h.customDimensions) x WHERE x.index = 22) as date_depart,
        (SELECT x.value FROM UNNEST(h.customDimensions) x WHERE x.index = 143) as duree_voyage,
        (SELECT x.value FROM UNNEST(h.customDimensions) x WHERE x.index = 25) as type_voyage,
		count(*) as searches
From {{ source('ga_tui_fr', 'ga_sessions_*') }}, 
date_range,
Unnest (hits) as h
where _table_suffix between start_date and end_date and h.eventInfo.eventCategory='Utilisation Moteur HP'
group by 1,2,3,4,5,6,7,8