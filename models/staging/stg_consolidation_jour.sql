{{ config(materialized="table") }}

with
data as (
    select
        cast(datedepart as date) as date,
        count(distinct numerodossier) as total_dossiers,
        count(
            distinct case when id_email_md5 is null then numerodossier end
        ) as clients_non_identifies,
        count(
            distinct case when id_email_md5 is not null then numerodossier end
        ) as clients_identifies,

        count(
            case when destination = 'ESPAGNE' then numerodossier end
        ) as top_1_espagne,
        count(
            case when destination = 'CRECE' then numerodossier end
        ) as top_2_grece,
        count(
            case when destination = 'ITALIE' then numerodossier end
        ) as top_3_italie,
        count(
            case when destination = 'MAROC' then numerodossier end
        ) as top_4_maroc,
        count(
            case when destination = 'FRANCE' then numerodossier end
        ) as top_5_france,
        count(
            case when destination = 'TUNISIE' then numerodossier end
        ) as top_6_tunisie,
        count(
            case
                when destination = 'REPUBLIQUE DOMINICAINE' then numerodossier
            end
        ) as top_7_republique_dominicaine,
        count(
            case when destination = 'MEXIQUE' then numerodossier end
        ) as top_8_mexique,
        count(
            case when destination = 'MARTINIQUE' then numerodossier end
        ) as top_9_martinique,
        count(case when destination = 'CUBA' then numerodossier end)
            as top_10_cuba,
        round(
            safe_divide(
                sum(safe_cast(cabrut as float64)), count(distinct numerodossier)
            ),
            2
        ) as panier_moy,
        round(
            sum(
                distinct case
                    when canalregroupe = 'TO Prod.'
                        then safe_cast(cabrut as float64)
                end
            ),
            2
        ) as ca_to_prod,
        round(
            sum(
                distinct case
                    when canalregroupe = ' Group & Collect.'
                        then safe_cast(cabrut as float64)
                end
            ),
            2
        ) as ca_group_collect,
        round(
            sum(
                distinct case
                    when canalregroupe = 'Franchised'
                        then safe_cast(cabrut as float64)
                end
            ),
            2
        ) as ca_franchised,
        round(
            sum(
                distinct case
                    when canalregroupe = 'Internet'
                        then safe_cast(cabrut as float64)
                end
            ),
            2
        ) as ca_internet,
        round(
            sum(
                distinct case
                    when
                        canalregroupe = 'Owned'
                        then safe_cast(cabrut as float64)
                end
            ),
            2
        ) as ca_owned,
        round(
            sum(
                distinct case
                    when canalregroupe = 'Third Party'
                        then safe_cast(cabrut as float64)
                end
            ),
            2
        ) as ca_third_party,
        round(
            sum(
                distinct case
                    when canalregroupe = 'Call Center'
                        then safe_cast(cabrut as float64)
                end
            ),
            2
        ) as ca_call_center,
        round(
            sum(
                distinct case
                    when canalregroupe = 'Non Renseigné'
                        then safe_cast(cabrut as float64)
                end
            ),
            2
        ) as ca_non_renseigne
    from {{ source("bq_data", "datamart_V_032022") }}
    where dateretour >= datedepart
    group by date
)

select
    date,
    total_dossiers,
    clients_non_identifies,
    clients_identifies,
    top_1_espagne,
    top_2_grece,
    top_3_italie,
    top_4_maroc,
    top_5_france,
    top_6_tunisie,
    top_7_republique_dominicaine,
    top_8_mexique,
    top_9_martinique,
    top_10_cuba,
    panier_moy,
    ca_to_prod,
    ca_group_collect,
    ca_franchised,
    ca_internet,
    ca_owned,
    ca_third_party,
    ca_call_center,
    ca_non_renseigne
from data
