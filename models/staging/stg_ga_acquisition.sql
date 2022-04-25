
select
      concat(substr(cast(date as string), 0, 7),'-01') as mois ,
      customChannelGrouping,
      sum (sessions) as sessions , 
      sum(bounces) as bounces, 
      sum(new_users) as new_users,       
      sum(nb_transaction) as nb_transaction_ga ,
      round(sum(Revenue),2) as revenue_ga
 from {{ref('stg_ga_sessions')}}
 group by 1,2