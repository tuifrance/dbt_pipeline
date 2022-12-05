{{
    config(
        materialized='table',
        labels={'type': 'crm', 'contains_pie': 'no', 'category': 'production'},
    )
}}

with
    data_behaviour as (

        select
            date,
            visitid,
            clientid,
            fullvisitorid,
            unique_visit_id,
            sessions,
            searches,
            product_page,
            search_page,
            product_page_clicks,
            tunnel_step_1,
            tunnel_step_2,
            tunnel_step_3,
            tunnel_step_4,
            bounces,
            new_users,
            nb_transaction,
            revenue
        from {{ ref('stg_ga_ab_test_behaviour_data') }}
    ),

    data_ab_test as (

        select unique_visit_id, eventcategory, eventaction, eventlabel
        from {{ ref('stg_ga_ab_test_dictionary') }}
    )
 
select 
    data_behaviour.date, 
    data_behaviour.visitid, 
    data_behaviour.clientid, 
    data_behaviour.fullvisitorid, 
    data_behaviour.sessions, 
    data_behaviour.searches, 
    data_behaviour.search_page, 
    data_behaviour.product_page, 
    data_behaviour.product_page_clicks, 
    data_behaviour.tunnel_step_1, 
    data_behaviour.tunnel_step_2, 
    data_behaviour.tunnel_step_3, 
    data_behaviour.tunnel_step_4, 
    data_behaviour.nb_transaction, 
    data_behaviour.revenue, 
    data_ab_test.eventcategory, 
    data_ab_test.eventaction as ab_test, 
    data_ab_test.eventlabel ab_test_variation
 from data_behaviour
 left join data_ab_test
 on data_behaviour.unique_visit_id = data_ab_test.unique_visit_id    


    
