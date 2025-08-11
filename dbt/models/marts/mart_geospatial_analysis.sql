{{ config(
    materialized='table',
    schema='gold',
    file_format='delta',
    location_root='s3://gold/geospatial_analysis/'
) }}

-- Geospatial Analysis Mart: Location-based business and economic insights
with regional_business_density as (
    select 
        -- Improve region classification
        case 
            when postal_region is null or postal_region = '' then 'UNCLASSIFIED'
            when postal_region = 'Unknown' then 'UNCLASSIFIED'
            else postal_region
        end as postal_region,
        count(*) as business_count,
        count(case when is_active then 1 end) as active_business_count,
        round(count(case when is_active then 1 end) * 100.0 / count(*), 2) as regional_activity_rate,
        avg(data_quality_score) as avg_data_quality,
        min(registration_year) as earliest_registration,
        max(registration_year) as latest_registration,
        count(distinct entity_category) as entity_categories,
        count(distinct business_age_category) as age_categories
    from {{ ref('stg_acra_companies') }}
    where registration_year >= 2015  -- Focus on recent data
    group by 
        case 
            when postal_region is null or postal_region = '' then 'UNCLASSIFIED'
            when postal_region = 'Unknown' then 'UNCLASSIFIED'
            else postal_region
        end
),

property_market_by_region as (
    select 
        region_category,
        avg(rental_median) as avg_rental_price,
        min(rental_median) as min_rental_price,
        max(rental_median) as max_rental_price,
        stddev(rental_median) as rental_price_volatility,
        count(*) as transaction_count,
        -- Only calculate averages for valid Singapore coordinates
        avg(case when latitude between 1.0 and 1.5 and longitude between 103.0 and 104.5 then latitude end) as avg_latitude,
        avg(case when latitude between 1.0 and 1.5 and longitude between 103.0 and 104.5 then longitude end) as avg_longitude,
        count(distinct property_category) as property_categories,
        count(distinct district) as districts,
        max(ref_year) as latest_year,
        count(distinct ref_year) as years_covered,
        count(case when latitude between 1.0 and 1.5 and longitude between 103.0 and 104.5 then 1 end) as valid_coordinates
    from {{ ref('stg_ura_rentals') }}
    where ref_year >= 2020 and rental_median is not null
    group by region_category
),

commercial_index_trends as (
    select 
        year,
        avg(rental_index_numeric) as avg_rental_index,
        min(rental_index_numeric) as min_rental_index,
        max(rental_index_numeric) as max_rental_index,
        count(*) as index_records,
        count(distinct property_category) as property_categories
    from {{ ref('stg_commercial_rental') }}
    where year is not null
    group by year
),

regional_economic_profile as (
    select 
        rbd.postal_region,
        
        -- Business metrics
        rbd.business_count as total_businesses,
        rbd.active_business_count as total_active_businesses,
        rbd.regional_activity_rate as avg_activity_rate,
        rbd.avg_data_quality as regional_data_quality,
        rbd.entity_categories as business_type_diversity,
        rbd.age_categories as business_age_diversity,
        
        -- Property market correlation (latest year)
        avg(pmr.avg_rental_price) as avg_regional_rental_price,
        avg(pmr.rental_price_volatility) as avg_rental_volatility,
        sum(pmr.transaction_count) as total_property_transactions,
        
        -- Geographic center
        avg(pmr.avg_latitude) as business_weighted_latitude,
        avg(pmr.avg_longitude) as business_weighted_longitude,
        
        -- Regional economic strength score (0-10)
        round(
            least(10, greatest(0,
                (rbd.active_business_count / 1000.0 * 3) +        -- Business density: 30%
                (rbd.regional_activity_rate / 100.0 * 2.5) +      -- Activity rate: 25%
                (rbd.entity_categories / 10.0 * 2) +              -- Diversity: 20%
                (least(5000, avg(pmr.avg_rental_price)) / 5000.0 * 1.5) + -- Market value: 15%
                (rbd.avg_data_quality * 1)                       -- Data quality: 10%
            )), 2
        ) as regional_economic_strength
        
    from regional_business_density rbd
    left join property_market_by_region pmr on (
        case 
            when rbd.postal_region = 'Central' then 'CENTRAL'
            when rbd.postal_region = 'North East' then 'NORTH'
            when rbd.postal_region = 'North West' then 'NORTH'
            when rbd.postal_region = 'South East' then 'SOUTH'
            when rbd.postal_region = 'South West' then 'SOUTH'
            when rbd.postal_region = 'West' then 'WEST'
            when rbd.postal_region = 'Central West' then 'WEST'
            else 'OTHER'
        end = pmr.region_category
    )
    group by 
        rbd.postal_region, rbd.business_count, rbd.active_business_count,
        rbd.regional_activity_rate, rbd.avg_data_quality, rbd.entity_categories, rbd.age_categories
),

district_level_insights as (
    select 
        pmr.region_category,
        
        -- Property market metrics (aggregated by region)
        avg(pmr.avg_rental_price) as district_avg_rental,
        avg(pmr.rental_price_volatility) as district_rental_volatility,
        sum(pmr.transaction_count) as district_transactions,
        pmr.districts as districts_count,
        pmr.property_categories as property_types,
        
        -- Geographic coordinates
        avg(pmr.avg_latitude) as district_latitude,
        avg(pmr.avg_longitude) as district_longitude,
        
        -- Time series trends (recent vs historical)
        avg(case when pmr.ref_year >= 2022 then pmr.avg_rental_price end) as recent_avg_rental,
        avg(case when pmr.ref_year between 2019 and 2021 then pmr.avg_rental_price end) as historical_avg_rental,
        
        -- Commercial index correlation
        avg(cit.avg_rental_index) as correlated_commercial_index,
        
        -- District attractiveness score (0-10)
        round(
            least(10, greatest(0,
                (sum(pmr.transaction_count) / 1000.0 * 4) +       -- Transaction volume: 40%
                (least(8000, avg(pmr.avg_rental_price)) / 8000.0 * 3) + -- Market value: 30%
                (10 - least(10, coalesce(avg(pmr.rental_price_volatility), 5))) * 0.2 + -- Stability: 20%
                (avg(cit.avg_rental_index) / 120.0 * 1)           -- Commercial strength: 10%
            )), 2
        ) as district_attractiveness_score
        
    from property_market_by_region pmr
    left join commercial_index_trends cit on pmr.ref_year = cit.year
    group by pmr.region_category, pmr.districts, pmr.property_categories
)

select 
    -- Regional business profile
    rep.postal_region,
    rep.total_businesses,
    rep.total_active_businesses,
    rep.avg_activity_rate,
    rep.business_type_diversity,
    rep.business_age_diversity,
    rep.regional_data_quality,
    rep.regional_economic_strength,
    
    -- Property market integration
    rep.avg_regional_rental_price,
    rep.avg_rental_volatility,
    rep.total_property_transactions,
    
    -- Geographic positioning
    rep.business_weighted_latitude,
    rep.business_weighted_longitude,
    
    -- District-level insights
    dli.district_avg_rental as avg_district_rental_in_region,
    dli.district_attractiveness_score as avg_district_attractiveness,
    dli.districts_count as districts_in_region,
    dli.property_types as property_categories_in_region,
    
    -- Trend analysis
    dli.recent_avg_rental as recent_rental_trend,
    dli.historical_avg_rental as historical_rental_trend,
    round(
        (dli.recent_avg_rental - dli.historical_avg_rental) * 100.0 / 
        nullif(dli.historical_avg_rental, 0), 2
    ) as rental_growth_rate_pct,
    
    -- Regional classification
    case 
        when rep.regional_economic_strength >= 8 then 'PRIME_BUSINESS_HUB'
        when rep.regional_economic_strength >= 6.5 then 'STRONG_BUSINESS_ZONE'
        when rep.regional_economic_strength >= 5 then 'MODERATE_BUSINESS_AREA'
        when rep.regional_economic_strength >= 3.5 then 'EMERGING_BUSINESS_REGION'
        else 'DEVELOPING_AREA'
    end as regional_business_classification,
    
    -- Analysis metadata
    current_timestamp as analysis_timestamp
    
from regional_economic_profile rep
left join district_level_insights dli on (
    case 
        when rep.postal_region = 'Central' then 'CENTRAL'
        when rep.postal_region in ('North East', 'North West') then 'NORTH'
        when rep.postal_region in ('South East', 'South West') then 'SOUTH'
        when rep.postal_region in ('West', 'Central West') then 'WEST'
        else 'OTHER'
    end = dli.region_category
)
where rep.total_businesses > 0
order by rep.regional_economic_strength desc, rep.total_active_businesses desc