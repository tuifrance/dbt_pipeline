{{
  config(
    materialized = 'table',
    labels = {'type': 'cdg_google_analytics', 'contains_pie': 'no', 'category':'production'}  
  )
}}

with data_ga as (
    select
        date,
        device,
        channelgrouping,
        campaign,
        medium,
        source,
        transactionid,
        type_voyage,
        user_id,
        eventcategory,
        eventaction,
        code_produit,
        code_parent_produit,
        nom_produit,
        destination as ga_destinatiion,
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
        --DateFinOption,
        datereservation,
        reservation_order,
        customer_type,
        --Date1ereConfirmation,
        --DateDepart,
        --DateRetour,
        --DateAnnulation,
        statutreservation,
        typedossier,
        codeagence,
        agence,
        codepointdevente,
        pointdevente,
        reseau,
        canal,
        canalregroupe,
        canalcalcule,
        distributionconso,
        canaldistributionniveau1,
        marque,
        codeproduit,
        produit,
        typeproduit,
        groupemarketingproduit,
        gamme,
        gammegroupe,
        codeiso3destination,
        destination,
        codevilledestination,
        codesejour,
        naturesejour,
        fournisseursejour,
        groupemarketingconsosejour,
        groupeconceptsejourconso,
        conceptsejourconso,
        villesejour,
        payssejour,
        codecircuit,
        circuit,
        naturecircuit,
        fournisseurcircuit,
        groupemarketingcircuit,
        paysarriveecircuit,
        villearriveecircuit,
        marquecircuit,
        villedepart,
        villearrivee,
        codeiso3paysvillearrivee,
        terminaldepart,
        terminalarrivee,
        flagrefpart,
        flagreductionrefpart,
        nbrclients,
        nbradultes,
        nbrenfants,
        nbrbebes,
        cabrut,
        dmaj,
        numero_transaction

    from {{ ref('stg_crm_data_overview') }}


)

select *
from data_ga
left join data_crm
    on data_ga.transactionid = data_crm.numerodossier
