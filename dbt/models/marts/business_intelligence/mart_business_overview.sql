{{ config(
    materialized='table',
    schema='bi'
) }}

with company_metrics as (
    select
        entity_category,
        postal_region,
        business_age_category,
        registration_year,
        registration_month,
        count(*) as total_companies,
        sum(case when is_active then 1 else 0 end) as active_companies,
        sum(case when not is_active then 1 else 0 end) as inactive_companies,
        round(avg(data_quality_score), 3) as avg_quality_score,
        count(case when quality_tier = 'HIGH' then 1 end) as high_quality_records,
        min(uen_issue_date) as earliest_registration,
        max(uen_issue_date) as latest_registration
    from {{ ref('stg_acra_companies') }}
    group by 1, 2, 3, 4, 5
),

economic_indicators as (
    select
        period_year,
        period_quarter,
        indicator_category,
        count(*) as total_indicators,
        count(distinct table_id) as unique_tables,
        count(distinct series_id) as unique_series,
        round(avg(value_numeric), 2) as avg_value,
        round(min(value_numeric), 2) as min_value,
        round(max(value_numeric), 2) as max_value,
        round(stddev(value_numeric), 2) as value_stddev
    from {{ ref('stg_singstat_economics') }}
    where period_year >= year(current_date()) - 5  -- Last 5 years
    group by 1, 2, 3
),

property_metrics as (
    select
        property_category,
        region,
        market_segment,
        development_era,
        built_year,
        count(*) as total_properties,
        sum(case when has_coordinates then 1 else 0 end) as properties_with_coordinates,
        count(case when tenure_category = 'FREEHOLD' then 1 end) as freehold_properties,
        count(case when tenure_category like 'LEASEHOLD%' then 1 end) as leasehold_properties,
        round(avg(property_age_years), 1) as avg_property_age,
        round(avg(estimated_lease_remaining_years), 1) as avg_lease_remaining
    from {{ ref('stg_ura_geospatial') }}
    where built_year >= 1960  -- Focus on modern developments
    group by 1, 2, 3, 4, 5
),

-- Cross-domain insights
business_property_correlation as (
    select
        c.postal_region,
        c.entity_category,
        p.property_category,
        p.market_segment,
        count(distinct c.uen) as companies_in_region,
        count(distinct p.project) as properties_in_region,
        round(avg(c.data_quality_score), 3) as avg_company_quality,
        round(avg(p.data_quality_score), 3) as avg_property_quality
    from {{ ref('stg_acra_companies') }} c
    left join {{ ref('stg_ura_geospatial') }} p
        on c.postal_region = p.region
    where c.is_active = true
    group by 1, 2, 3, 4
),

-- Time series trends
registration_trends as (
    select
        registration_year,
        registration_month,
        entity_category,
        count(*) as monthly_registrations,
        sum(count(*)) over (
            partition by registration_year, entity_category 
            order by registration_month 
            rows unbounded preceding
        ) as cumulative_yearly_registrations
    from {{ ref('stg_acra_companies') }}
    where registration_year >= year(current_date()) - 10
    group by 1, 2, 3
),

-- Final aggregated view
business_overview as (
    select
        current_timestamp() as report_generated_at,
        
        -- Company metrics summary
        (
            select sum(total_companies) 
            from company_metrics
        ) as total_registered_companies,
        
        (
            select sum(active_companies) 
            from company_metrics
        ) as total_active_companies,
        
        (
            select round(avg(avg_quality_score), 3) 
            from company_metrics
        ) as overall_company_data_quality,
        
        -- Economic indicators summary
        (
            select count(distinct indicator_category) 
            from economic_indicators
        ) as tracked_economic_categories,
        
        (
            select sum(total_indicators) 
            from economic_indicators
        ) as total_economic_data_points,
        
        -- Property metrics summary
        (
            select sum(total_properties) 
            from property_metrics
        ) as total_tracked_properties,
        
        (
            select sum(properties_with_coordinates) 
            from property_metrics
        ) as properties_with_geolocation,
        
        (
            select round(avg(avg_property_age), 1) 
            from property_metrics
        ) as average_property_age_years,
        
        -- Cross-domain insights
        (
            select count(distinct postal_region) 
            from business_property_correlation
        ) as regions_with_business_property_data,
        
        -- Data freshness indicators
        (
            select max(silver_processed_timestamp) 
            from {{ ref('stg_acra_companies') }}
        ) as latest_company_data_update,
        
        (
            select max(silver_processed_timestamp) 
            from {{ ref('stg_singstat_economics') }}
        ) as latest_economic_data_update,
        
        (
            select max(silver_processed_timestamp) 
            from {{ ref('stg_ura_geospatial') }}
        ) as latest_property_data_update
)

select * from business_overview

-- This model provides a comprehensive business intelligence overview
-- combining insights from all three data sources for executive dashboards