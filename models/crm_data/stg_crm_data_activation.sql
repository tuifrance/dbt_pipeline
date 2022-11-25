{{
    config(
        materialized='table',
        labels={'type': 'crm', 'contains_pie': 'no', 'category': 'production'},
    )
}}

select
    email,
    count(distinct numerodossier) as transactions,
    case
        when count(distinct numerodossier) = 1 then 'Achat Unique' else 'Multi Acheteur'
    end as type_achat,
    count(distinct datereservation) as nb_jour_resa,
    min(datereservation) as min_date_resa,
    max(datereservation) as max_date_resa,
    date_diff(current_date(), min(datereservation), day) as seniorite_day,
    round(date_diff(current_date(), min(datereservation), day)/365,2) as seniorite_year,
    date_diff(current_date(), max(datereservation), day) as recence_day,
    round(date_diff(current_date(), max(datereservation), day )/365,2) as recence_year,
    count(
        distinct case when statutreservation = 'Ferme' then numerodossier end
    ) as transactions_ferme,
    count(
        distinct case when statutreservation != 'Ferme' then numerodossier end
    ) as transactions_annule,
    count(
        distinct case when canalregroupe = 'Internet' then numerodossier end
    ) as transactions_internet,
    count(
        distinct case when canalregroupe != 'Internet' then numerodossier end
    ) as transactions_hors_web,
    round(avg(nbrclients), 2) as avg_nbclients,
    round(min(nbrclients), 2) as min_nbclients,
    round(min(nbrclients), 2) as max_nbclients,
    round(avg(cabrut), 2) as avg_ca_brut,
    round(sum(cabrut), 2) as sum_ca_brut,
    round(min(cabrut), 2) as min_ca_brut,
    round(max(cabrut), 2) as max_ca_brut,
from {{ ref('stg_crm_data_overview') }}
group by 1
order by transactions desc
