{{ config(materialized='table') }}

with
 data as(
     SELECT id, 
     name, 
     city, 
     address_full 
     FROM  {{ source('bq_partoo', 'business_analytics') }}),

 data1 as (
     select * from {{ source('bq_partoo', 'presence_analytics') }})
select 
  data1.business_id, 
  CONCAT(name, '/', city) AS store_ville, 
  name, 
  city, 
  address_full, 
  date, 
  queries_direct, 
  queries_indirect, 
  queries_discovery, 
  queries_branded, 
  views_maps, 
  views_search, 
  actions_website, 
  actions_phone, 
  actions_driving_directions, 
  bucket 
from 
  data 
  left join data1 on data.id = data1.business_id
