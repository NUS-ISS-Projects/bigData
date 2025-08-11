-- Business Intelligence Mart: Comprehensive business landscape analysis
-- Materialized as Delta Lake table in gold schema

with company_metrics as (
    select 
        postal_region,
        entity_category,
        business_age_category,
        count(*) as total_companies,
        count(case when is_active then 1 end) as active_companies,
        round(avg(data_quality_score), 3) as avg_data_quality,
        min(registration_year) as earliest_registration,
        max(registration_year) as latest_registration_year
    from {{ ref('stg_acra_companies') }}
    group by postal_region, entity_category, business_age_category
),

economic_summary as (
     select 
         period_year,
         avg(value_numeric) as avg_economic_value,
         count(*) as indicator_count
     from {{ ref('stg_singstat_economics') }}
     where value_numeric is not null
     group by period_year
 ),

government_summary as (
     select 
         financial_year,
         sum(amount_million_numeric) as total_expenditure,
         count(*) as expenditure_records
     from {{ ref('stg_government_expenditure') }}
     where amount_million_numeric is not null
     group by financial_year
 ),

property_summary as (
    select 
        region_category,
        avg(rental_median) as avg_rental_price,
        count(*) as rental_transactions
    from {{ ref('stg_ura_rentals') }}
    where rental_median is not null
    group by region_category
)

select 
    cm.postal_region,
    cm.entity_category,
    cm.business_age_category,
    cm.total_companies,
    cm.active_companies,
    round(cm.active_companies * 100.0 / cm.total_companies, 2) as activity_rate,
    cm.avg_data_quality,
    
    -- Economic context
    es.avg_economic_value,
    es.indicator_count,
    
    -- Government investment
    gs.total_expenditure,
    gs.expenditure_records,
    
    -- Property market correlation
    ps.avg_rental_price,
    ps.rental_transactions,
    
    -- Improved business intelligence score (0-100) without rental component
    round(
        least(100, greatest(0,
            -- Company activity component (50% - increased weight)
            (cm.active_companies / 100.0 * 50) +
            -- Data quality component (25%)
            (cm.avg_data_quality * 25) +
            -- Economic context component (25%)
            (coalesce(es.avg_economic_value, 0) / 1000000.0 * 25)
            -- Removed rental component due to 96.9% missing data
        )), 2
    ) as business_intelligence_score,
    
    current_timestamp as analysis_timestamp
    
from company_metrics cm
left join economic_summary es on cm.latest_registration_year = es.period_year
left join government_summary gs on cm.latest_registration_year = gs.financial_year
left join property_summary ps on (
    case 
        when cm.postal_region = 'Central' then 'CENTRAL'
        when cm.postal_region in ('North East', 'North West') then 'NORTH'
        when cm.postal_region in ('South East', 'South West') then 'SOUTH'
        when cm.postal_region in ('West', 'Central West') then 'WEST'
        else 'OTHER'
    end = ps.region_category
)
order by business_intelligence_score desc, cm.active_companies desc