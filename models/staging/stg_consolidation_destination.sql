{{ config(materialized='table') }}
with data as (
    select
        destination,
        count(
            distinct case
                when statutreservation = 'Ferme' then numerodossier
            end
        ) as total_dossier_ferme,
        count(
            distinct case when canalregroupe = 'TO Prod.' then numerodossier end
        ) as to_prod,
        count(
            distinct case
                when canalregroupe = ' Group & Collect.' then numerodossier
            end
        ) as group_collect,
        count(
            distinct case
                when canalregroupe = 'Franchised' then numerodossier
            end
        ) as franchised,
        count(
            distinct case when canalregroupe = 'Internet' then numerodossier end
        ) as internet,
        count(distinct case when canalregroupe = 'Owned' then numerodossier end)
            as owned,
        count(
            distinct case
                when canalregroupe = 'Third Party' then numerodossier
            end
        ) as third_party,
        count(
            distinct case
                when canalregroupe = 'Call Center' then numerodossier
            end
        ) as call_center,
        count(
            distinct case
                when canalregroupe = 'Non Renseigné' then numerodossier
            end
        ) as non_renseigne,
        count(distinct case when id_email_md5 is null then numerodossier end)
            as clients_non_identifies,
        count(
            distinct case when id_email_md5 is not null then numerodossier end
        )
            as clients_identifies,
        round(
            safe_divide(
                sum(safe_cast(cabrut as FLOAT64)), count(distinct numerodossier)
            ),
            2
        ) as panier_moy,
        round(
            avg(
                date_diff(
                    cast(datedepart as Date), cast(datereservation as Date), day
                )
            ),
            2
        ) as delai_achat,
        round(
            avg(
                date_diff(
                    cast(dateretour as Date), cast(datedepart as Date), day
                )
            ),
            2
        ) as moy_dure_sejour,
        count(
            distinct case
                when typeproduit = 'Sejour Balneaire' then numerodossier
            end
        ) as sejour_balneaire,
        count(distinct case when typeproduit = 'Circuit' then numerodossier end)
            as circuit,
        count(
            distinct case when typeproduit = 'Vols secs' then numerodossier end
        )
            as vols_secs,
        count(
            distinct case
                when typeproduit = 'Sejour_Neige' then numerodossier
            end
        ) as sejour_neige,
        count(
            distinct case
                when typeproduit = 'Sejour Ville' then numerodossier
            end
        ) as sejour_ville,
        count(
            distinct case
                when typeproduit = 'Sejour Nature' then numerodossier
            end
        ) as sejour_nature,
        count(
            distinct case when typeproduit = 'Autotour' then numerodossier end
        )
            as autotour,
        count(
            distinct case when typeproduit = 'Croisiere' then numerodossier end
        )
            as croisiere,
        round(avg(nbrclients), 2) as pax_moy,
        round(
            sum(nbradultes) / sum(case when nbrenfants > 0 then nbrenfants end),
            2
        ) as ratio_adultes_enfants,
        count(
            distinct case
                when groupemarketingcircuit = 'Decouvrir' then numerodossier
            end
        ) as decourir,
        count(
            distinct case
                when groupemarketingcircuit = 'CONNAISSEUR' then numerodossier
            end
        ) as connaisseur,
        count(
            distinct case
                when groupemarketingcircuit = 'APPROFONDIR' then numerodossier
            end
        ) as approfondir,
        count(
            distinct case
                when groupemarketingcircuit = 'ESSENTIEL' then numerodossier
            end
        ) as essentiel,
        count(
            distinct case
                when
                    groupemarketingcircuit = 'Séjours Activités'
                    then numerodossier
            end
        ) as sejours_activites,
        count(
            distinct case
                when
                    groupemarketingcircuit = 'GRANDEUR NATURE'
                    then numerodossier
            end
        ) as grandeur_nature,
        count(
            distinct case
                when groupemarketingcircuit = 'RENCONTRER' then numerodossier
            end
        ) as rencontrer,
        count(
            distinct case
                when groupemarketingcircuit = 'ESTHETE' then numerodossier
            end
        ) as esthete,
        count(
            distinct case
                when groupemarketingcircuit = 'HOTELS AUTRES' then numerodossier
            end
        ) as hotels_autres,
        count(
            distinct case
                when groupemarketingcircuit = 'Lookéa' then numerodossier
            end
        ) as lookea,
        count(
            distinct case
                when
                    groupemarketingcircuit = 'Tours & Circuits'
                    then numerodossier
            end
        ) as tours_circuit,
        count(
            distinct case
                when groupemarketingcircuit = 'SENSATIONNEL' then numerodossier
            end
        ) as sensationnel
    from {{ source('bq_data', 'datamart_V_032022') }}
    where dateretour >= datedepart
    group by destination
)

select
    destination,
    total_dossier_ferme,
    to_prod,
    group_collect,
    franchised,
    internet,
    owned,
    third_party,
    call_center,
    non_renseigne,
    clients_non_identifies,
    clients_identifies,
    panier_moy,
    delai_achat,
    moy_dure_sejour,
    sejour_balneaire,
    circuit,
    vols_secs,
    sejour_neige,
    sejour_ville,
    sejour_nature,
    autotour,
    croisiere,
    pax_moy,
    ratio_adultes_enfants,
    decourir,
    connaisseur,
    approfondir,
    essentiel,
    sejours_activites,
    grandeur_nature,
    rencontrer,
    esthete,
    hotels_autres,
    lookea,
    tours_circuit,
    sensationnel
from data
