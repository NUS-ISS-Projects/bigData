{{ config(
    materialized='table',
    schema='gold',
    file_format='delta',
    location_root='s3://gold/economic_analysis/'
) }}

-- Economic Analysis Mart: Economic trends and business formation correlations
-- Fixed to properly aggregate at year level
with economic_trends as (
    select 
        period_year,
        avg(case when indicator_category = 'GDP' then value_numeric end) as gdp_indicator,
        avg(case when indicator_category = 'INFLATION' then value_numeric end) as inflation_indicator,
        avg(case when indicator_category = 'UNEMPLOYMENT' then value_numeric end) as unemployment_indicator,
        avg(case when indicator_category = 'TRADE' then value_numeric end) as trade_indicator,
        avg(case when indicator_category = 'HOUSING' then value_numeric end) as housing_indicator,
        count(*) as total_indicators,
        avg(value_numeric) as avg_value,
        count(distinct indicator_category) as data_points,
        stddev(value_numeric) as value_volatility
    from {{ ref('stg_singstat_economics') }}
    where period_year between 2000 and 2024 and value_numeric is not null  -- Improved date range
    group by period_year
),

business_formation_trends as (
    select 
        registration_year,
        count(*) as new_businesses,
        count(case when is_active then 1 end) as active_new_businesses,
        round(avg(data_quality_score), 3) as avg_data_quality,
        count(distinct entity_category) as entity_categories
    from {{ ref('stg_acra_companies') }}
    where registration_year between 2015 and 2024
    group by registration_year
),

government_investment as (
    select 
        financial_year,
        sum(amount_million_numeric) as total_investment,
        avg(amount_million_numeric) as avg_investment,
        count(*) as investment_items,
        count(distinct expenditure_category) as expenditure_categories
    from {{ ref('stg_government_expenditure') }}
    where financial_year between 2015 and 2024
      and amount_million_numeric is not null
    group by financial_year
),

property_market_health as (
    select 
        ref_year,
        avg(rental_median) as avg_rental_price,
        count(*) as market_transactions,
        count(distinct district) as active_districts,
        count(distinct property_category) as property_categories
    from {{ ref('stg_ura_rentals') }}
    where ref_year between 2015 and 2024 and rental_median is not null
    group by ref_year
),

commercial_market_index as (
    select 
        year,
        avg(rental_index_numeric) as avg_rental_index,
        stddev(rental_index_numeric) as rental_index_volatility,
        count(*) as index_observations,
        count(distinct property_category) as commercial_property_types
    from {{ ref('stg_commercial_rental') }}
    where year between 2015 and 2024 and rental_index_numeric is not null
    group by year
),

yearly_economic_summary as (
    select 
        et.period_year as year,
        
        -- Economic indicators
        et.avg_value,
        et.data_points,
        et.value_volatility,
        et.gdp_indicator,
        et.inflation_indicator,
        et.unemployment_indicator,
        et.trade_indicator,
        et.housing_indicator,
        
        -- Business formation metrics
        bft.new_businesses,
        bft.active_new_businesses,
        round(bft.active_new_businesses * 100.0 / nullif(bft.new_businesses, 0), 2) as business_survival_rate,
        bft.avg_data_quality,
        
        -- Government investment
        gi.total_investment,
        gi.avg_investment,
        gi.investment_items,
        
        -- Property market health
        pmh.avg_rental_price,
        pmh.market_transactions,
        pmh.active_districts,
        
        -- Commercial market stability
        cmi.avg_rental_index,
        cmi.rental_index_volatility,
        
        -- Improved economic health score (0-10) with better scaling
        round((
            -- Business formation component (40% - increased weight)
            (least(10, coalesce(bft.active_new_businesses / 500.0, 0)) * 0.4) +
            -- Economic stability component (35% - increased weight)
            (least(10, coalesce(et.avg_value / 5000.0, 0)) * 0.35) +
            -- Government investment component (25%)
            (least(10, coalesce(gi.total_investment / 5000.0, 0)) * 0.25)
            -- Removed market activity due to data sparsity
        ), 2) as economic_health_score
        
    from economic_trends et
    left join business_formation_trends bft on et.period_year = bft.registration_year
    left join government_investment gi on et.period_year = gi.financial_year
    left join property_market_health pmh on et.period_year = pmh.ref_year
    left join commercial_market_index cmi on et.period_year = cmi.year
)

select 
    year,
    avg_value,
    data_points,
    value_volatility,
    gdp_indicator,
    inflation_indicator,
    unemployment_indicator,
    trade_indicator,
    housing_indicator,
    new_businesses,
    active_new_businesses,
    business_survival_rate,
    avg_data_quality,
    total_investment,
    avg_investment,
    investment_items,
    avg_rental_price,
    market_transactions,
    active_districts,
    avg_rental_index,
    rental_index_volatility,
    economic_health_score,
    
    -- Analysis metadata
    current_timestamp as analysis_timestamp,
    case 
        when economic_health_score >= 8 then 'EXCELLENT'
        when economic_health_score >= 6 then 'GOOD'
        when economic_health_score >= 4 then 'MODERATE'
        when economic_health_score >= 2 then 'CONCERNING'
        else 'POOR'
    end as economic_health_category
    
from yearly_economic_summary
where year is not null
order by year desc