{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

select 
       string_field_0 as destination, 
       string_field_1 as city, 
       string_field_2 as reference, 
       string_field_3 as code_produit, 
       string_field_4 as type, 
       string_field_5 as flexi, 
  from {{ source('product', 'product_data') }}
  where string_field_0!='destination'