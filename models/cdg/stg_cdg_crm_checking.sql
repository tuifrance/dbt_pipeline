{{
    config(
        materialized="table",
        labels={"type": "cdg", "contains_pie": "no", "category": "production"},
    )
}}

with
    data_cdg as (
        select
            mois_de_reservation,
            semaine_de_reservation,
            red__part_aire_y,
            date_de_reservation,
            numero_dossier,
            mois_de_depart,
            date_de_depart,
            package__y_n_,
            marque,
            groupe_marketing_produit,
            ag_ce_consolidee_dossier_viaxeo,
            ag_ce_detaillee,
            point_de_v_te,
            code_ville_depart_tussy,
            pays_destination_consolide_finance,
            destination_to_produit,
            code_ville_arrivee_tussy,
            code_produit,
            produit,
            categorie_crm_produit,
            promotion__y_n_,
            groupe_duree_de_sejour_detail__en_,
            duree_de_sejour,
            dossier_a_valoir__y_n_,
            a_valoir_genere__y_n_,
            dossier_report_suite_covid19__y_n_,
            nb_cli_ts_dossier_finance,
            ca_brut
        from {{ ref("stg_cdg_overview") }}
    ),
    data_crm as (

        select distinct
            id_email_md5,
            numerodossier,
            datereservation,
            customer_type,
            statutreservation,
            pointdevente,
            reseau,
            canal,
            canalregroupe,
            canalcalcule,
            distributionconso,
            canaldistributionniveau1,
            codeproduit,
        from {{ ref("stg_crm_data_overview") }}
    )

select
    * except (customer_type),
    case
        when customer_type is null then 'Unknown' else customer_type
    end as customer_type,
from data_cdg
left join data_crm on data_cdg.numero_dossier = data_crm.numerodossier
order by data_cdg.date_de_reservation desc
