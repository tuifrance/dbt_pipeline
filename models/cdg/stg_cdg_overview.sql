{{
    config(
        materialized='table',
        labels={'type': 'cdg', 'contains_pie': 'no', 'category': 'production'},
    )
}}
select
    cast(mois_de_reservation as date) as mois_de_reservation,
    semaine_de_reservation,
    red__part_aire_y,
    cast(date_de_reservation as date) as date_de_reservation,
    numero_dossier,
    cast(mois_de_depart as date) as mois_de_depart,
    cast(date_de_depart as date) as date_de_depart,
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
    cast(nb_cli_ts_dossier_finance as float64) as nb_cli_ts_dossier_finance,
    ca_brut

from {{ source('cdg', 'historic_new_cdg') }}
order by date_de_reservation desc
