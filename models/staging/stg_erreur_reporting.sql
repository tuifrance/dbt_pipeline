{{ config(materialized = 'table') }}
with
errors as (
    select
        date,
        h.eventinfo.eventcategory,
        (
            select MAX(IF(index = 29, value, ""))
            from UNNEST(h.customdimensions)
        ) as product_id,
        REGEXP_EXTRACT(h.eventinfo.eventaction, ".*:OZXFT_(.*):paxAdult.*")
            as codetussy,
        REGEXP_EXTRACT(
            h.eventinfo.eventaction, ".*&departuredate=(.*)&duration=.*"
        ) as depart,
        REGEXP_EXTRACT(
            h.eventinfo.eventaction, ".*?departurecitycode=(.*)&productcode=.*"
        ) as villedepart,
        REGEXP_EXTRACT(
            h.eventinfo.eventaction, ".*&duration=(.*)&roomcode=.*"
        ) as duree,
        REGEXP_EXTRACT(h.eventinfo.eventaction, ".*:paxAdult_(.*):paxChild.*")
            as adultes,
        REGEXP_EXTRACT(
            h.eventinfo.eventaction, ".*:paxChild_(.*):paxInfant.*"
        ) as enfants,
        REGEXP_EXTRACT(h.eventinfo.eventaction, ".*:paxInfant_(.*)") as bebe,
        h.eventinfo.eventaction as pageerror,
        COUNT(*) as errors
    from {{ source('ga_tui_fr', 'ga_sessions_*') }},
        UNNEST(ga.hits) as h
    where
        _table_suffix between FORMAT_DATE(
            "%Y%m%d", DATE_SUB(CURRENT_DATE(), interval 1 day)
        )
        and FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), interval 1 day))
        and h.eventinfo.eventcategory = "Pages Erreur"
        and REGEXP_EXTRACT(
            h.eventinfo.eventaction, ".*:OZXFT_(.*):paxAdult.*"
        ) is not null
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11
),

product_info as (
    select *
    from {{ source('bq_data_erreur', 'product_data') }}
)

select
    date,
    eventcategory,
    product_id,
    codetussy,
    villedepart,
    duree,
    adultes,
    enfants,
    bebe,
    pageerror,
    errors,
    string_field_0 as destination,
    string_field_1 as city,
    string_field_2 as nomproduit,
    string_field_4 as typeproduit,
    string_field_5 as flex,
    CONCAT(
        SUBSTR(depart, 0, 2),
        "/",
        SUBSTR(depart, 3, 2),
        "/",
        SUBSTR(depart, 5, 4)
    ) as datedepart,
    case
        when string_field_3 is null then "Produit non dispo" else
            "Produit dispo"
    end as produitdispo
from
    errors
left join
    product_info
    on
        errors.product_id = product_info.string_field_3
limit 500
/* limit added automatically by dbt cloud */
