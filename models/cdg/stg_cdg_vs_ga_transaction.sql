{{ config(materialized="table") }}

with
ga_transactions as (
    select
        date,
        device,
        channelgrouping,
        campaign,
        medium,
        `source`,
        keyword,
        adcontent,
        transactionid,
        type_voyage,
        user_id,
        eventcategory,
        eventaction,
        code_produit as code_produit_ga,
        code_parent_produit,
        nom_produit,
        destination,
        ville_depart,
        ville_arrive,
        pax_total,
        pax_adult,
        pax_bebe,
        duree_sejour,
        continent,
        type_paiement,
        prixproduit,
        revenue
    from {{ ref("stg_ga_transactions_daily") }}
),

cdg_transactions as (
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
)

select
    *,
    case when transactionid is null then 'FALSE' else 'TRUE' end
        as retrieved_status,
    case
        when lower(eventcategory) like '%postbooking%' then 'PostBooking' else
            'Booking'
    end as booking_status
from cdg_transactions
left join ga_transactions
    on cdg_transactions.numero_dossier = ga_transactions.transactionid
