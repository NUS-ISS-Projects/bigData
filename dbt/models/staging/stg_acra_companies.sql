{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from delta.`s3a://silver/acra_companies_clean`
),

staging as (
    select
        uen,
        entity_name,
        entity_type,
        entity_status,
        is_active,
        uen_issue_date,
        reg_street_name,
        reg_postal_code,
        data_quality_score,
        source,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(bronze_ingestion_timestamp as timestamp) as bronze_ingestion_timestamp,
        cast(silver_processed_timestamp as timestamp) as silver_processed_timestamp,
        
        -- Derived fields
        case 
            when entity_type in ('COMPANY', 'PRIVATE COMPANY LIMITED BY SHARES') then 'PRIVATE_COMPANY'
            when entity_type in ('PUBLIC COMPANY', 'PUBLIC COMPANY LIMITED BY SHARES') then 'PUBLIC_COMPANY'
            when entity_type in ('LIMITED LIABILITY PARTNERSHIP', 'LLP') then 'LLP'
            when entity_type in ('PARTNERSHIP', 'GENERAL PARTNERSHIP') then 'PARTNERSHIP'
            when entity_type in ('SOLE PROPRIETORSHIP', 'BUSINESS') then 'SOLE_PROPRIETORSHIP'
            else 'OTHER'
        end as entity_category,
        
        case 
            when reg_postal_code is not null then 
                case 
                    when reg_postal_code between '010000' and '199999' then 'Central'
                    when reg_postal_code between '200000' and '299999' then 'North East'
                    when reg_postal_code between '300000' and '399999' then 'North West'
                    when reg_postal_code between '400000' and '499999' then 'South East'
                    when reg_postal_code between '500000' and '599999' then 'South West'
                    when reg_postal_code between '600000' and '699999' then 'West'
                    when reg_postal_code between '700000' and '799999' then 'Central West'
                    else 'Unknown'
                end
            else 'Unknown'
        end as postal_region,
        
        extract(year from uen_issue_date) as registration_year,
        extract(month from uen_issue_date) as registration_month,
        
        case 
            when data_quality_score >= 0.9 then 'HIGH'
            when data_quality_score >= 0.7 then 'MEDIUM'
            when data_quality_score >= 0.5 then 'LOW'
            else 'VERY_LOW'
        end as quality_tier,
        
        -- Business age calculation
        datediff(current_date(), uen_issue_date) as days_since_registration,
        
        case 
            when datediff(current_date(), uen_issue_date) <= 365 then 'NEW_BUSINESS'
            when datediff(current_date(), uen_issue_date) <= 1825 then 'YOUNG_BUSINESS'  -- 5 years
            when datediff(current_date(), uen_issue_date) <= 3650 then 'MATURE_BUSINESS'  -- 10 years
            else 'ESTABLISHED_BUSINESS'
        end as business_age_category
        
    from source_data
    where data_quality_score >= {{ var('min_quality_score') }}
)

select * from staging

-- Data quality tests will be defined in schema.yml