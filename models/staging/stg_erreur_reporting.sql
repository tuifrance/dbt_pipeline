 {{ config(materialized = 'table') }}
with 
  errors AS (
  SELECT date, h.eventInfo.eventCategory,( SELECT MAX(IF(index=29,value,""))
   FROM UNNEST(h.customDimensions)) AS PRODUCT_ID,
    REGEXP_EXTRACT(h.eventInfo.eventAction, '.*:OZXFT_(.*):paxAdult.*') AS CodeTussy,
    REGEXP_EXTRACT(h.eventInfo.eventAction, '.*&departuredate=(.*)&duration=.*') AS Depart,
    REGEXP_EXTRACT(h.eventInfo.eventAction, '.*?departurecitycode=(.*)&productcode=.*') AS VilleDepart,
    REGEXP_EXTRACT(h.eventInfo.eventAction, '.*&duration=(.*)&roomcode=.*') AS Duree,
    REGEXP_EXTRACT(h.eventInfo.eventAction, '.*:paxAdult_(.*):paxChild.*') AS Adultes,
    REGEXP_EXTRACT(h.eventInfo.eventAction, '.*:paxChild_(.*):paxInfant.*') AS Enfants,
    REGEXP_EXTRACT(h.eventInfo.eventAction, '.*:paxInfant_(.*)') AS Bebe,
    h.eventInfo.eventAction AS PageError,
    COUNT(*) AS Errors
    from {{ source('ga_tui_fr', 'ga_sessions_*') }} as GA,
    UNNEST(GA.hits) AS h
  WHERE
  _table_suffix between FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))AND h.eventInfo.eventCategory ='Pages Erreur'
  and REGEXP_EXTRACT(h.eventInfo.eventAction, '.*:OZXFT_(.*):paxAdult.*') IS NOT NULL
  GROUP BY
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
    11),
  product_info AS (
  SELECT
    *
     from {{ source('bq_data_erreur', 'product_data') }} )
SELECT
  date,
  eventCategory,
  PRODUCT_ID,
  CodeTussy,
  CONCAT ( substr(Depart,0,2),'/',substr(Depart,3,2),'/',substr(Depart,5,4)) as dateDepart,
  VilleDepart,
  Duree,
  Adultes,
  Enfants,
  Bebe,
  PageError,
  Errors,
  string_field_0 AS destination,
  string_field_1 AS city,
  string_field_2 as nomProduit,
  string_field_4 as typeProduit,
  string_field_5 as Flex,
  CASE WHEN string_field_3 is null then 'Produit non dispo' else 'Produit dispo' END as produitDispo
FROM
  errors
LEFT JOIN
  product_info
ON
  errors.PRODUCT_ID = product_info.string_field_3
limit 500
/* limit added automatically by dbt cloud */