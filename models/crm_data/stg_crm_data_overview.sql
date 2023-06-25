{{
  config(
    materialized = 'table',
    labels = {'type': 'crm', 'contains_pie': 'no', 'category':'production'}  
  )
}}

select distinct
    ID_EMAIL_MD5,
    NUMERODOSSIER,
    cast(DATERESERVATION as date) as DATERESERVATION,
    --cast( (case DateFinOption in ('nan','non') then '1990-01-01' else DateFinOption end ) as date) as DateFinOption,
    cast(DATEDEPART as date) as DATEDEPART,
    STATUTRESERVATION,
    TYPEDOSSIER,
    --cast(Date1ereConfirmation as date) as Date1ereConfirmation,
    CODEAGENCE,
    --cast(DateRetour as date) as DateRetour,
    --cast(DateAnnulation as date) as DateAnnulation,
    AGENCE,
    CODEPOINTDEVENTE,
    POINTDEVENTE,
    RESEAU,
    CANAL,
    CANALREGROUPE,
    CANALCALCULE,
    DISTRIBUTIONCONSO,
    CANALDISTRIBUTIONNIVEAU1,
    MARQUE,
    CODEPRODUIT,
    PRODUIT,
    TYPEPRODUIT,
    GROUPEMARKETINGPRODUIT,
    GAMME,
    GAMMEGROUPE,
    CODEISO3DESTINATION,
    DESTINATION,
    CODEVILLEDESTINATION,
    CODESEJOUR,
    NATURESEJOUR,
    FOURNISSEURSEJOUR,
    GROUPEMARKETINGCONSOSEJOUR,
    GROUPECONCEPTSEJOURCONSO,
    CONCEPTSEJOURCONSO,
    VILLESEJOUR,
    PAYSSEJOUR,
    CODECIRCUIT,
    CIRCUIT,
    NATURECIRCUIT,
    FOURNISSEURCIRCUIT,
    GROUPEMARKETINGCIRCUIT,
    PAYSARRIVEECIRCUIT,
    VILLEARRIVEECIRCUIT,
    MARQUECIRCUIT,
    VILLEDEPART,
    VILLEARRIVEE,
    CODEISO3PAYSVILLEARRIVEE,
    TERMINALDEPART,
    TERMINALARRIVEE,
    FLAGREFPART,
    FLAGREDUCTIONREFPART,
    NBRCLIENTS,
    NBRADULTES,
    NBRENFANTS,
    NBRBEBES,
    cast(replace(CABRUT, ',', '.') as float64) as CABRUT,
    DMAJ,
    lower(EMAIL) as EMAIL,
    rank()
        over (partition by ID_EMAIL_MD5 order by DATERESERVATION asc)
        as RESERVATION_ORDER,
    case
        when ID_EMAIL_MD5 = 'nan' then 'Unknown'
        when
            rank() over (partition by ID_EMAIL_MD5 order by DATERESERVATION asc)
            = 1
            then 'New Customer'
        when
            rank() over (partition by ID_EMAIL_MD5 order by DATERESERVATION asc)
            > 1
            then 'Old Customer'
    end as CUSTOMER_TYPE,
    rank()
        over (partition by EMAIL order by cast(DATERESERVATION as date) asc)
        as NUMERO_TRANSACTION
from {{ source('crm', 'WS_DIGITAL_DATAMART_DOSSIER') }}
where ID_EMAIL_MD5 is not null
order by DATERESERVATION desc
