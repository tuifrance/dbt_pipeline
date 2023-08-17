{{ config(materialized='table') }}
with data as (
    select
        id_email_md5,
        numerodossier,
        datereservation,
        statutreservation
    from {{ source('bq_data', 'datamart_V_032022') }}
    where statutreservation = 'Ferme' and id_email_md5 is not null
    group by 1, 2, 3, 4
    order by 3
)

select
    id_email_md5,
    numerodossier,
    datereservation,
    statutreservation,
    dateresa_inf,
    round(
        avg(
            date_diff(
                cast(datereservation as Date), cast(dateresa_inf as Date), day
            )
        ),
        2
    ) as delai_achat,
    row_number()
        over (partition by id_email_md5 order by datereservation)
        as nb_achat
from (select
    *,
    lag(datereservation)
        over (partition by id_email_md5 order by datereservation)
        as dateresa_inf
from data)
group by 1, 2, 3, 4, 5
order by 7 desc
