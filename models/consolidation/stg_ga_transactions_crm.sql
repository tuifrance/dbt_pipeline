{{
  config(
    materialized = 'table',
    labels = {'type': 'crm_google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

with data_transaction as (
    select distinct
        date,
        device,
        channelgrouping,
        campaign,
        medium,
        source,
        keyword,
        adcontent,
        transactionid,
        type_voyage,
        user_id,
        eventcategory,
        eventaction,
        code_produit,
        code_parent_produit,
        nom_produit,
        --destination,
        ville_depart,
        ville_arrive,
        pax_total,
        pax_adult,
        pax_enfant,
        pax_bebe,
        duree_sejour,
        continent,
        type_paiement,
        prixproduit,
        revenue
    from {{ ref('stg_ga_transactions_daily') }}
),

data_crm as (

    select
        id_email_md5,
        numerodossier,
        statutreservation,
        typedossier,
        codeagence,
        reseau,
        canalregroupe,
        marque,
        codeproduit,
        produit,
        typeproduit,
        groupemarketingproduit,
        gamme,
        gammegroupe,
        destination,
        codesejour,
        naturesejour,
        fournisseursejour,
        groupemarketingconsosejour,
        groupeconceptsejourconso,
        conceptsejourconso,
        villesejour,
        payssejour,
        villedepart,
        villearrivee,
        terminaldepart,
        terminalarrivee,
        nbrclients,
        cabrut,
        customer_type
    from {{ ref('stg_crm_data_overview') }}
)

select *
from data_transaction
left join data_crm
    on data_transaction.transactionid = data_crm.numerodossier
order by data_transaction.date desc
