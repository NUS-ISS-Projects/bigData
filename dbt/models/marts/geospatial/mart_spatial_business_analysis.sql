{{ config(
    materialized='table',
    schema='geo'
) }}

with property_analysis as (
    select
        region,
        district,
        property_category,
        market_segment,
        tenure_category,
        development_era,
        
        -- Property metrics
        count(*) as total_properties,
        sum(case when has_coordinates then 1 else 0 end) as properties_with_coordinates,
        round(avg(property_age_years), 1) as avg_property_age,
        round(avg(estimated_lease_remaining_years), 1) as avg_lease_remaining,
        
        -- Property value proxies
        count(case when property_category = 'LANDED' then 1 end) as landed_properties,
        count(case when property_category = 'CONDOMINIUM' then 1 end) as condo_properties,
        count(case when property_category = 'HDB' then 1 end) as hdb_properties,
        count(case when tenure_category = 'FREEHOLD' then 1 end) as freehold_properties,
        
        -- Development timeline
        min(built_year) as earliest_development,
        max(built_year) as latest_development,
        
        -- Coordinate bounds for mapping
        min(x_coordinate) as min_x_coord,
        max(x_coordinate) as max_x_coord,
        min(y_coordinate) as min_y_coord,
        max(y_coordinate) as max_y_coord,
        
        -- Data quality
        round(avg(data_quality_score), 3) as avg_property_data_quality
        
    from {{ ref('stg_ura_geospatial') }}
    where region != 'OTHER'
    group by 1, 2, 3, 4, 5, 6
),

business_analysis as (
    select
        postal_region as region,
        entity_category,
        business_age_category,
        
        -- Business metrics
        count(*) as total_businesses,
        sum(case when is_active then 1 else 0 end) as active_businesses,
        round(avg(days_since_registration), 0) as avg_business_age_days,
        
        -- Business type distribution
        count(case when entity_category = 'PRIVATE_COMPANY' then 1 end) as private_companies,
        count(case when entity_category = 'SOLE_PROPRIETORSHIP' then 1 end) as sole_proprietorships,
        count(case when entity_category = 'LLP' then 1 end) as llps,
        count(case when entity_category = 'PARTNERSHIP' then 1 end) as partnerships,
        
        -- Registration trends
        count(case when registration_year >= year(current_date()) - 1 then 1 end) as recent_registrations,
        count(case when registration_year >= year(current_date()) - 5 then 1 end) as last_5_years_registrations,
        
        -- Business maturity
        count(case when business_age_category = 'NEW_BUSINESS' then 1 end) as new_businesses,
        count(case when business_age_category = 'ESTABLISHED_BUSINESS' then 1 end) as established_businesses,
        
        -- Data quality
        round(avg(data_quality_score), 3) as avg_business_data_quality
        
    from {{ ref('stg_acra_companies') }}
    where postal_region != 'Unknown'
    group by 1, 2, 3
),

-- Regional business-property correlation
regional_correlation as (
    select
        coalesce(p.region, b.region) as region,
        
        -- Property metrics summary
        sum(p.total_properties) as total_properties_in_region,
        sum(p.properties_with_coordinates) as properties_with_coordinates,
        round(avg(p.avg_property_age), 1) as region_avg_property_age,
        sum(p.landed_properties) as total_landed_properties,
        sum(p.condo_properties) as total_condo_properties,
        sum(p.hdb_properties) as total_hdb_properties,
        sum(p.freehold_properties) as total_freehold_properties,
        
        -- Business metrics summary
        sum(b.total_businesses) as total_businesses_in_region,
        sum(b.active_businesses) as active_businesses_in_region,
        sum(b.private_companies) as total_private_companies,
        sum(b.sole_proprietorships) as total_sole_proprietorships,
        sum(b.recent_registrations) as recent_business_registrations,
        sum(b.new_businesses) as new_businesses_in_region,
        sum(b.established_businesses) as established_businesses_in_region,
        
        -- Calculated ratios and insights
        case 
            when sum(p.total_properties) > 0 then round(sum(b.total_businesses)::float / sum(p.total_properties), 3)
            else 0
        end as business_to_property_ratio,
        
        case 
            when sum(b.total_businesses) > 0 then round(sum(b.active_businesses)::float / sum(b.total_businesses) * 100, 1)
            else 0
        end as business_activity_rate_pct,
        
        case 
            when sum(p.total_properties) > 0 then round(sum(p.freehold_properties)::float / sum(p.total_properties) * 100, 1)
            else 0
        end as freehold_property_pct,
        
        case 
            when sum(b.total_businesses) > 0 then round(sum(b.private_companies)::float / sum(b.total_businesses) * 100, 1)
            else 0
        end as private_company_pct,
        
        -- Economic development indicators
        case 
            when sum(b.recent_registrations) > sum(b.total_businesses) * 0.1 then 'HIGH_GROWTH'
            when sum(b.recent_registrations) > sum(b.total_businesses) * 0.05 then 'MODERATE_GROWTH'
            when sum(b.recent_registrations) > 0 then 'SLOW_GROWTH'
            else 'STAGNANT'
        end as business_growth_category,
        
        case 
            when sum(p.total_properties) > 1000 and sum(b.total_businesses) > 500 then 'MAJOR_HUB'
            when sum(p.total_properties) > 500 and sum(b.total_businesses) > 200 then 'SIGNIFICANT_CENTER'
            when sum(p.total_properties) > 100 and sum(b.total_businesses) > 50 then 'DEVELOPING_AREA'
            else 'EMERGING_AREA'
        end as development_classification,
        
        -- Data quality assessment
        round(avg(coalesce(p.avg_property_data_quality, 0)), 3) as avg_property_data_quality,
        round(avg(coalesce(b.avg_business_data_quality, 0)), 3) as avg_business_data_quality
        
    from property_analysis p
    full outer join business_analysis b
        on p.region = b.region
    where coalesce(p.region, b.region) is not null
    group by 1
),

-- Property type business affinity analysis
property_business_affinity as (
    select
        p.property_category,
        p.market_segment,
        b.entity_category,
        
        count(*) as co_location_instances,
        round(avg(b.avg_business_age_days), 0) as avg_business_age_in_property_type,
        sum(b.active_businesses) as active_businesses_near_property_type,
        
        -- Affinity score (normalized co-location frequency)
        round(
            count(*)::float / 
            (select count(*) from business_analysis) * 1000, 2
        ) as affinity_score
        
    from property_analysis p
    inner join business_analysis b
        on p.region = b.region
    group by 1, 2, 3
),

-- Final spatial business intelligence
spatial_business_intelligence as (
    select
        rc.*,
        
        -- Enhanced insights
        case 
            when rc.business_to_property_ratio > 0.5 then 'BUSINESS_DENSE'
            when rc.business_to_property_ratio > 0.2 then 'BALANCED'
            when rc.business_to_property_ratio > 0.1 then 'RESIDENTIAL_FOCUSED'
            else 'LOW_BUSINESS_DENSITY'
        end as area_character,
        
        case 
            when rc.business_activity_rate_pct > 90 then 'VERY_ACTIVE'
            when rc.business_activity_rate_pct > 80 then 'ACTIVE'
            when rc.business_activity_rate_pct > 70 then 'MODERATELY_ACTIVE'
            else 'LOW_ACTIVITY'
        end as business_vitality,
        
        -- Investment attractiveness score (composite)
        round(
            (rc.business_activity_rate_pct * 0.3 +
             rc.freehold_property_pct * 0.2 +
             rc.private_company_pct * 0.2 +
             case rc.business_growth_category
                 when 'HIGH_GROWTH' then 25
                 when 'MODERATE_GROWTH' then 15
                 when 'SLOW_GROWTH' then 5
                 else 0
             end * 0.3), 1
        ) as investment_attractiveness_score,
        
        current_timestamp() as analysis_timestamp
        
    from regional_correlation rc
)

select * from spatial_business_intelligence
order by investment_attractiveness_score desc, total_businesses_in_region desc

-- This model provides comprehensive spatial business intelligence
-- combining property development patterns with business formation
-- for location-based strategic decision making