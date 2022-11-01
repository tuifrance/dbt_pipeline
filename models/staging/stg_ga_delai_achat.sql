{{ config(materialized='table') }}
with data as (SELECT  
ID_EMAIL_MD5,
NumeroDossier,
DateReservation, 
statutReservation
FROM {{ source('bq_data', 'datamart_V_032022') }}  
where statutReservation ='Ferme' and ID_EMAIL_MD5 is not null
group by 1,2,3,4
order by 3)

select ID_EMAIL_MD5,
NumeroDossier,
DateReservation, 
statutReservation,
DateResa_inf, 
round(AVG(DATE_DIFF(cast(DateReservation as Date), cast(DateResa_inf as Date), day)),2) as delai_achat,
ROW_NUMBER() over ( partition by ID_EMAIL_MD5 order by DateReservation) as nb_achat,
from (select *,
lag(DateReservation) over ( partition by ID_EMAIL_MD5 order by DateReservation) as DateResa_inf, 
from data )
group by  1,2,3,4,5
order by 7 desc
