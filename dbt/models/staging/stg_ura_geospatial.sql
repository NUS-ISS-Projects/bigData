{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from delta.`s3a://silver/ura_geospatial_clean`
),

staging as (
    select
        project,
        street,
        x_coordinate,
        y_coordinate,
        has_coordinates,
        lease_commence_date,
        property_type,
        district,
        tenure,
        built_year,
        data_quality_score,
        source,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(bronze_ingestion_timestamp as timestamp) as bronze_ingestion_timestamp,
        cast(silver_processed_timestamp as timestamp) as silver_processed_timestamp,
        
        -- Derived fields
        case 
            when upper(property_type) like '%HDB%' or upper(property_type) like '%FLAT%' then 'HDB'
            when upper(property_type) like '%CONDOMINIUM%' or upper(property_type) like '%CONDO%' then 'CONDOMINIUM'
            when upper(property_type) like '%LANDED%' or upper(property_type) like '%TERRACE%' or upper(property_type) like '%DETACHED%' then 'LANDED'
            when upper(property_type) like '%EXECUTIVE%' then 'EXECUTIVE_CONDOMINIUM'
            when upper(property_type) like '%APARTMENT%' then 'APARTMENT'
            else 'OTHER'
        end as property_category,
        
        case 
            when upper(tenure) like '%FREEHOLD%' then 'FREEHOLD'
            when upper(tenure) like '%LEASEHOLD%' or upper(tenure) like '%LEASE%' then 'LEASEHOLD'
            when upper(tenure) like '%999%' then 'LEASEHOLD_999'
            else 'OTHER'
        end as tenure_category,
        
        -- District grouping based on Singapore planning areas
        case 
            when district in ('01', '02', '03', '04', '05', '06', '07', '08') then 'CENTRAL'
            when district in ('09', '10', '11', '12', '13', '14', '15', '16') then 'EAST'
            when district in ('17', '18', '19', '20', '21', '22', '23') then 'NORTH'
            when district in ('24', '25', '26', '27', '28') then 'WEST'
            else 'OTHER'
        end as region,
        
        -- Property age calculation
        case 
            when built_year is not null then year(current_date()) - built_year
            else null
        end as property_age_years,
        
        case 
            when built_year is not null then
                case 
                    when year(current_date()) - built_year <= 5 then 'NEW'
                    when year(current_date()) - built_year <= 15 then 'MODERN'
                    when year(current_date()) - built_year <= 30 then 'MATURE'
                    else 'OLD'
                end
            else 'UNKNOWN'
        end as property_age_category,
        
        -- Lease remaining calculation (approximate)
        case 
            when lease_commence_date is not null and upper(tenure) like '%99%' then
                99 - (year(current_date()) - year(lease_commence_date))
            when lease_commence_date is not null and upper(tenure) like '%LEASE%' then
                greatest(0, 99 - (year(current_date()) - year(lease_commence_date)))
            else null
        end as estimated_lease_remaining_years,
        
        case 
            when data_quality_score >= 0.9 then 'HIGH'
            when data_quality_score >= 0.7 then 'MEDIUM'
            when data_quality_score >= 0.5 then 'LOW'
            else 'VERY_LOW'
        end as quality_tier,
        
        -- Coordinate validation
        case 
            when x_coordinate is not null and y_coordinate is not null then
                case 
                    when x_coordinate between 2000 and 50000 and y_coordinate between 15000 and 55000 then 'VALID_SVY21'
                    when x_coordinate between 103.6 and 104.0 and y_coordinate between 1.2 and 1.5 then 'VALID_WGS84'
                    else 'INVALID_COORDINATES'
                end
            else 'NO_COORDINATES'
        end as coordinate_system,
        
        -- Development era classification
        case 
            when built_year < 1960 then 'PRE_INDEPENDENCE'
            when built_year between 1960 and 1979 then 'EARLY_DEVELOPMENT'
            when built_year between 1980 and 1999 then 'RAPID_GROWTH'
            when built_year between 2000 and 2009 then 'MILLENNIUM'
            when built_year >= 2010 then 'MODERN'
            else 'UNKNOWN'
        end as development_era,
        
        -- Market segment classification
        case 
            when upper(property_type) like '%HDB%' then 'PUBLIC_HOUSING'
            when upper(property_type) like '%EXECUTIVE%' then 'SEMI_PRIVATE'
            when upper(property_type) like '%CONDOMINIUM%' or upper(property_type) like '%APARTMENT%' then 'PRIVATE_NON_LANDED'
            when upper(property_type) like '%LANDED%' or upper(property_type) like '%TERRACE%' or upper(property_type) like '%DETACHED%' then 'PRIVATE_LANDED'
            else 'OTHER'
        end as market_segment
        
    from source_data
    where data_quality_score >= {{ var('min_quality_score') }}
      and (built_year is null or built_year between 1900 and year(current_date()))
)

select * from staging

-- Data quality tests will be defined in schema.yml