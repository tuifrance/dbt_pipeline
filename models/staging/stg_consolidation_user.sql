{{ config(materialized='table') }}


-- récupérer les top destination
With data1 As (
    Select
        id_email_md5,
        destination,
        count(destination) As nb
    From {{ source('bq_data', 'datamart_V_032022') }}
    Group By id_email_md5, destination
),

rang As (
    Select
        id_email_md5,
        destination,
        nb,
        row_number()
            Over (Partition By id_email_md5 Order By nb Desc)
            As row_number
    From data1
    Where id_email_md5 Is Not Null
    Order By id_email_md5
),

top_destination As (
    Select
        id_email_md5,
        top_destination_1,
        top_destination_2,
        top_destination_3
    From (
        Select
            id_email_md5,
            Case When row_number = 1 Then destination End As top_destination_1,
            Case When row_number = 2 Then destination End As top_destination_2,
            Case When row_number = 3 Then destination End As top_destination_3
        From rang
    )
),

-- récupérer les top canal
data2 As (
    Select
        id_email_md5,
        canalregroupe,
        count(canalregroupe) As nb
    From {{ source('bq_data', 'datamart_V_032022') }}
    Group By id_email_md5, canalregroupe
),

rang1 As (
    Select
        id_email_md5,
        canalregroupe,
        nb,
        row_number()
            Over (Partition By id_email_md5 Order By nb Desc)
            As row_number
    From data2
    Where id_email_md5 Is Not Null
    Order By id_email_md5
),

top_canal As (
    Select
        id_email_md5,
        top_canal_1,
        top_canal_2,
        top_canal_3
    From (
        Select
            id_email_md5,
            Case When row_number = 1 Then canalregroupe End As top_canal_1,
            Case When row_number = 2 Then canalregroupe End As top_canal_2,
            Case When row_number = 3 Then canalregroupe End As top_canal_3
        From rang1
    )
),

data As (

    Select
        id_email_md5,
        count(Distinct numerodossier) As total_dossier,
        count(Case When statutreservation = 'Ferme' Then numerodossier End)
            As total_dossier_ferme,
        count(
            Distinct Case
                When
                    statutreservation = ' Option'
                    Or statutreservation = 'Option annulée'
                    Then numerodossier
            End
        ) As total_option,
        count(
            Distinct Case
                When statutreservation = 'Option annulée' Then numerodossier
            End
        ) As total_option_annule,
        round(sum(safe_cast(cabrut As FLOAT64)), 2) As total_ca,
        count(Distinct destination) As total_destination,
        round(
            avg(
                date_diff(
                    cast(dateretour As Date), cast(datedepart As Date), Day
                )
            ),
            2
        ) As moy_dure_sejour,
        round(
            sum(
                date_diff(
                    cast(dateretour As Date), cast(datedepart As Date), Day
                )
            ),
            2
        ) As total_duree_sejour,
        count(Distinct typeproduit) As total_produit,
        count(
            Distinct Case
                When typeproduit = 'Sejour Balneaire' Then numerodossier
            End
        ) As sejour_balneaire,
        count(
            Distinct Case When typeproduit = 'Circuit' Then numerodossier End
        ) As circuit,
        count(
            Distinct Case When typeproduit = 'Vols secs' Then numerodossier End
        ) As vols_secs,
        count(
            Distinct Case
                When typeproduit = 'Sejour_Neige' Then numerodossier
            End
        ) As sejour_neige,
        count(
            Distinct Case
                When typeproduit = 'Sejour Ville' Then numerodossier
            End
        ) As sejour_ville,
        count(
            Distinct Case
                When typeproduit = 'Sejour Nature' Then numerodossier
            End
        ) As sejour_nature,
        count(
            Distinct Case When typeproduit = 'Autotour' Then numerodossier End
        ) As autotour,
        count(
            Distinct Case When typeproduit = 'Croisiere' Then numerodossier End
        ) As croisiere,
        count(
            Distinct Case When statutreservation = 'ferme' Then id_email_md5 End
        ) As nbr_achat_different,
        min(datereservation) As date_premiere_achat,
        max(datereservation) As date_dermiere_achat,
        date_diff(current_date(), max(cast(datereservation As Date)), Day)
            As recence,
        date_diff(current_date(), min(cast(datereservation As Date)), Day)
            As anciennete,
        round(
            avg(Case When statutreservation = 'ferme' Then nbrclients End), 2
        ) As moy_clients,
        Case When sum(nbrenfants) > 0 Then 1 Else 0 End As avec_sans_enfant,
        Case
            When
                count(Case When destination = 'France' Then 1 Else 0 End) > 1
                Then 1
            Else 0
        End As sejour_france,
        round(
            avg(
                date_diff(
                    cast(datedepart As Date), cast(datereservation As Date), Day
                )
            ),
            2
        ) As delai_depart,
        round(
            safe_divide(
                sum(safe_cast(cabrut As FLOAT64)), count(Distinct numerodossier)
            ),
            2
        ) As panier_moy,
        count(
            Distinct Case
                When
                    extract(Month From cast(datedepart As DATE)) In (1, 2)
                    Then numerodossier
            End
        ) As dossier_hiver,
        count(
            Distinct Case
                When
                    extract(Month From cast(datedepart As DATE)) In (6, 7)
                    Then numerodossier
            End
        ) As dossier_ete,
        max(safe_cast(cabrut As FLOAT64)) As max_depense,
        min(safe_cast(cabrut As FLOAT64)) As min_depense
    From {{ source('bq_data', 'datamart_V_032022') }}
    Where id_email_md5 Is Not Null And dateretour >= datedepart
    Group By id_email_md5
)

Select
    data.id_email_md5,
    total_dossier,
    total_dossier As ferme,
    total_option,
    total_option_annule,
    total_ca,
    total_destination,
    moy_dure_sejour,
    total_produit,
    sejour_balneaire,
    circuit,
    vols_secs,
    sejour_neige,
    sejour_ville,
    sejour_nature,
    autotour,
    croisiere,
    nbr_achat_different,
    date_premiere_achat,
    date_dermiere_achat,
    recence,
    anciennete,
    moy_clients,
    avec_sans_enfant,
    top_destination_1,
    top_destination_2,
    top_destination_3,
    top_canal_1,
    top_canal_2,
    top_canal_3,
    sejour_france,
    delai_depart,
    panier_moy,
    dossier_hiver,
    dossier_ete,
    max_depense,
    min_depense
From data
Left Join top_destination As t1 On data.id_email_md5 = t1.id_email_md5
Left Join top_canal As t2 On data.id_email_md5 = t2.id_email_md5
Order By data.id_email_md5
