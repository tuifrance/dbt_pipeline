{{
    config(
        materialized='table',
        labels={
            'type': 'google_analytics',
            'contains_pie': 'no',
            'category': 'production',
        },
    )
}}

with
    data_fiche_produit as (
        select
            date,
            code_produit,
            concat(date, '_', code_produit) as unique_ligne_id,
            sum(consultation) as consultations,
            sum(voir_tarif) as click_voir_tarif,
            sum(ville_depart) as click_ville_depart,
            sum(duration) as click_duration,
            sum(gofunnel) as click_go_funnel
        from {{ ref('stg_ga_fiche_produit') }}
        group by 1, 2, 3
    ),
    data_cdg as (
        select
            date_de_reservation,
            code_produit as cdg_code_produit,
            concat(date_de_reservation, '_', code_produit) as unique_ligne_id,
            produit as cdg_produit,
            categorie_crm_produit as category,
            destination_to as destination,
            count(distinct numero_dossier) as cdg_ventes,
            sum(nb_cli_ts_dossier_finance) as cdg_pax,
            sum(ca_brut) as cdg_revenue
        from {{ ref('stg_cdg_overview') }}
        group by 1, 2, 3, 4, 5, 6

    ),

    product_data as (
        select
            destination as prd_destination,
            city as prd_city,
            reference as prd_reference,
            code_produit as prd_code_produit,
            type as prd_type,
            flexi as prd_flexi,
        from {{ ref('stg_product_reference') }}
    )

select
    date_de_reservation,
    cdg_code_produit,
    cdg_produit,
    category,
    destination,
    cdg_ventes,
    cdg_revenue,
    code_produit,
    consultations,
    prd_destination,
    prd_city,
    prd_reference,
    prd_code_produit,
    prd_type,
    prd_flexi

from data_cdg
left join
    data_fiche_produit on data_cdg.unique_ligne_id = data_fiche_produit.unique_ligne_id
left join product_data on cdg_code_produit = prd_code_produit
order by consultations desc
