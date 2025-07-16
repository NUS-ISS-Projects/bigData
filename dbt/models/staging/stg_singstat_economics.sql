{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from delta.`s3a://silver/singstat_economics_clean`
),

staging as (
    select
        table_id,
        series_id,
        data_type,
        period,
        period_year,
        period_quarter,
        value_original,
        value_numeric,
        is_valid_numeric,
        unit,
        data_quality_score,
        source,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(bronze_ingestion_timestamp as timestamp) as bronze_ingestion_timestamp,
        cast(silver_processed_timestamp as timestamp) as silver_processed_timestamp,
        
        -- Derived fields
        case 
            when table_id like '%GDP%' or series_id like '%GDP%' then 'GDP'
            when table_id like '%CPI%' or series_id like '%CPI%' or table_id like '%INFLATION%' then 'INFLATION'
            when table_id like '%UNEMPLOYMENT%' or series_id like '%UNEMPLOYMENT%' then 'UNEMPLOYMENT'
            when table_id like '%INTEREST%' or series_id like '%INTEREST%' then 'INTEREST_RATE'
            when table_id like '%POPULATION%' or series_id like '%POPULATION%' then 'DEMOGRAPHICS'
            when table_id like '%TRADE%' or series_id like '%TRADE%' or table_id like '%EXPORT%' or table_id like '%IMPORT%' then 'TRADE'
            when table_id like '%HOUSING%' or series_id like '%HOUSING%' or table_id like '%PROPERTY%' then 'HOUSING'
            when table_id like '%MANUFACTURING%' or series_id like '%MANUFACTURING%' then 'MANUFACTURING'
            when table_id like '%SERVICES%' or series_id like '%SERVICES%' then 'SERVICES'
            else 'OTHER'
        end as indicator_category,
        
        case 
            when period_quarter is not null then concat(cast(period_year as string), '-Q', cast(period_quarter as string))
            when period_year is not null then cast(period_year as string)
            else period
        end as standardized_period,
        
        case 
            when period_quarter is not null then 'QUARTERLY'
            when period_year is not null and period_quarter is null then 'ANNUAL'
            when period like '%M%' then 'MONTHLY'
            else 'OTHER'
        end as frequency,
        
        case 
            when data_quality_score >= 0.9 then 'HIGH'
            when data_quality_score >= 0.7 then 'MEDIUM'
            when data_quality_score >= 0.5 then 'LOW'
            else 'VERY_LOW'
        end as quality_tier,
        
        -- Time-based categorization
        case 
            when period_year >= year(current_date()) - 1 then 'RECENT'
            when period_year >= year(current_date()) - 5 then 'CURRENT_CYCLE'
            when period_year >= year(current_date()) - 10 then 'HISTORICAL'
            else 'ARCHIVE'
        end as data_recency,
        
        -- Unit standardization
        case 
            when upper(unit) like '%PERCENT%' or upper(unit) like '%PCT%' or unit = '%' then 'PERCENTAGE'
            when upper(unit) like '%DOLLAR%' or upper(unit) like '%SGD%' or unit like '$%' then 'CURRENCY_SGD'
            when upper(unit) like '%MILLION%' then 'MILLIONS'
            when upper(unit) like '%BILLION%' then 'BILLIONS'
            when upper(unit) like '%THOUSAND%' then 'THOUSANDS'
            when upper(unit) like '%INDEX%' then 'INDEX'
            when upper(unit) like '%RATE%' then 'RATE'
            else 'OTHER'
        end as unit_category,
        
        -- Value range categorization for analysis
        case 
            when value_numeric is null then 'NO_VALUE'
            when value_numeric = 0 then 'ZERO'
            when value_numeric > 0 and value_numeric <= 1 then 'VERY_LOW'
            when value_numeric > 1 and value_numeric <= 10 then 'LOW'
            when value_numeric > 10 and value_numeric <= 100 then 'MEDIUM'
            when value_numeric > 100 and value_numeric <= 1000 then 'HIGH'
            when value_numeric > 1000 then 'VERY_HIGH'
            when value_numeric < 0 then 'NEGATIVE'
            else 'OTHER'
        end as value_range
        
    from source_data
    where data_quality_score >= {{ var('min_quality_score') }}
      and is_valid_numeric = true
      and period_year between {{ var('start_date')[:4] | int }} and {{ var('end_date')[:4] | int }}
)

select * from staging

-- Data quality tests will be defined in schema.yml