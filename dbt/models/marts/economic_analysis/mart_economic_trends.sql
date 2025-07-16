{{ config(
    materialized='table',
    schema='economics'
) }}

with gdp_data as (
    select
        period_year,
        period_quarter,
        standardized_period,
        value_numeric as gdp_value,
        unit_category,
        series_id
    from {{ ref('stg_singstat_economics') }}
    where indicator_category = 'GDP'
      and frequency in ('QUARTERLY', 'ANNUAL')
      and period_year >= 2010
),

inflation_data as (
    select
        period_year,
        period_quarter,
        standardized_period,
        value_numeric as inflation_rate,
        unit_category,
        series_id
    from {{ ref('stg_singstat_economics') }}
    where indicator_category = 'INFLATION'
      and frequency in ('QUARTERLY', 'ANNUAL')
      and period_year >= 2010
),

unemployment_data as (
    select
        period_year,
        period_quarter,
        standardized_period,
        value_numeric as unemployment_rate,
        unit_category,
        series_id
    from {{ ref('stg_singstat_economics') }}
    where indicator_category = 'UNEMPLOYMENT'
      and frequency in ('QUARTERLY', 'ANNUAL')
      and period_year >= 2010
),

trade_data as (
    select
        period_year,
        period_quarter,
        standardized_period,
        value_numeric as trade_value,
        unit_category,
        series_id,
        case 
            when upper(series_id) like '%EXPORT%' then 'EXPORT'
            when upper(series_id) like '%IMPORT%' then 'IMPORT'
            else 'TRADE_GENERAL'
        end as trade_type
    from {{ ref('stg_singstat_economics') }}
    where indicator_category = 'TRADE'
      and frequency in ('QUARTERLY', 'ANNUAL')
      and period_year >= 2010
),

-- Business registration trends as economic indicator
business_formation as (
    select
        registration_year as period_year,
        registration_month,
        entity_category,
        count(*) as new_registrations,
        sum(case when is_active then 1 else 0 end) as active_new_registrations
    from {{ ref('stg_acra_companies') }}
    where registration_year >= 2010
    group by 1, 2, 3
),

-- Quarterly business formation aggregation
quarterly_business_formation as (
    select
        period_year,
        case 
            when registration_month between 1 and 3 then 1
            when registration_month between 4 and 6 then 2
            when registration_month between 7 and 9 then 3
            when registration_month between 10 and 12 then 4
        end as period_quarter,
        entity_category,
        sum(new_registrations) as quarterly_registrations,
        sum(active_new_registrations) as quarterly_active_registrations
    from business_formation
    group by 1, 2, 3
),

-- Combined economic indicators
economic_indicators_combined as (
    select
        coalesce(g.period_year, i.period_year, u.period_year, t.period_year) as period_year,
        coalesce(g.period_quarter, i.period_quarter, u.period_quarter, t.period_quarter) as period_quarter,
        
        -- GDP metrics
        avg(g.gdp_value) as avg_gdp_value,
        count(g.gdp_value) as gdp_data_points,
        
        -- Inflation metrics
        avg(i.inflation_rate) as avg_inflation_rate,
        count(i.inflation_rate) as inflation_data_points,
        
        -- Unemployment metrics
        avg(u.unemployment_rate) as avg_unemployment_rate,
        count(u.unemployment_rate) as unemployment_data_points,
        
        -- Trade metrics
        sum(case when t.trade_type = 'EXPORT' then t.trade_value end) as total_exports,
        sum(case when t.trade_type = 'IMPORT' then t.trade_value end) as total_imports,
        sum(t.trade_value) as total_trade_value,
        count(t.trade_value) as trade_data_points
        
    from gdp_data g
    full outer join inflation_data i
        on g.period_year = i.period_year 
        and g.period_quarter = i.period_quarter
    full outer join unemployment_data u
        on coalesce(g.period_year, i.period_year) = u.period_year 
        and coalesce(g.period_quarter, i.period_quarter) = u.period_quarter
    full outer join trade_data t
        on coalesce(g.period_year, i.period_year, u.period_year) = t.period_year 
        and coalesce(g.period_quarter, i.period_quarter, u.period_quarter) = t.period_quarter
    group by 1, 2
),

-- Economic trends with business formation
economic_trends as (
    select
        e.period_year,
        e.period_quarter,
        concat(cast(e.period_year as string), '-Q', cast(e.period_quarter as string)) as period_label,
        
        -- Economic indicators
        e.avg_gdp_value,
        e.avg_inflation_rate,
        e.avg_unemployment_rate,
        e.total_exports,
        e.total_imports,
        case 
            when e.total_imports > 0 then round(e.total_exports / e.total_imports, 3)
            else null
        end as trade_balance_ratio,
        
        -- Business formation metrics
        sum(bf.quarterly_registrations) as total_new_businesses,
        sum(bf.quarterly_active_registrations) as total_active_new_businesses,
        sum(case when bf.entity_category = 'PRIVATE_COMPANY' then bf.quarterly_registrations end) as new_private_companies,
        sum(case when bf.entity_category = 'SOLE_PROPRIETORSHIP' then bf.quarterly_registrations end) as new_sole_proprietorships,
        
        -- Calculate period-over-period changes
        lag(e.avg_gdp_value) over (order by e.period_year, e.period_quarter) as prev_gdp_value,
        lag(e.avg_inflation_rate) over (order by e.period_year, e.period_quarter) as prev_inflation_rate,
        lag(e.avg_unemployment_rate) over (order by e.period_year, e.period_quarter) as prev_unemployment_rate,
        lag(sum(bf.quarterly_registrations)) over (order by e.period_year, e.period_quarter) as prev_new_businesses,
        
        -- Data quality indicators
        e.gdp_data_points,
        e.inflation_data_points,
        e.unemployment_data_points,
        e.trade_data_points
        
    from economic_indicators_combined e
    left join quarterly_business_formation bf
        on e.period_year = bf.period_year 
        and e.period_quarter = bf.period_quarter
    where e.period_year is not null and e.period_quarter is not null
    group by 1, 2, 3, 4, 5, 6, 7, 8, 15, 16, 17, 18
),

-- Final economic trends with calculated changes
final_trends as (
    select
        *,
        
        -- Period-over-period percentage changes
        case 
            when prev_gdp_value > 0 then round(((avg_gdp_value - prev_gdp_value) / prev_gdp_value) * 100, 2)
            else null
        end as gdp_growth_rate_pct,
        
        case 
            when prev_inflation_rate is not null then round(avg_inflation_rate - prev_inflation_rate, 2)
            else null
        end as inflation_change_pct,
        
        case 
            when prev_unemployment_rate is not null then round(avg_unemployment_rate - prev_unemployment_rate, 2)
            else null
        end as unemployment_change_pct,
        
        case 
            when prev_new_businesses > 0 then round(((total_new_businesses - prev_new_businesses) / prev_new_businesses) * 100, 2)
            else null
        end as business_formation_growth_pct,
        
        -- Economic health indicators
        case 
            when avg_gdp_value > prev_gdp_value and avg_unemployment_rate < prev_unemployment_rate then 'POSITIVE'
            when avg_gdp_value < prev_gdp_value and avg_unemployment_rate > prev_unemployment_rate then 'NEGATIVE'
            else 'MIXED'
        end as economic_sentiment,
        
        -- Business environment indicator
        case 
            when total_new_businesses > prev_new_businesses and avg_unemployment_rate < prev_unemployment_rate then 'FAVORABLE'
            when total_new_businesses < prev_new_businesses and avg_unemployment_rate > prev_unemployment_rate then 'CHALLENGING'
            else 'NEUTRAL'
        end as business_environment,
        
        current_timestamp() as analysis_timestamp
        
    from economic_trends
)

select * from final_trends
order by period_year desc, period_quarter desc

-- This model provides comprehensive economic trend analysis
-- combining macroeconomic indicators with business formation data
-- for strategic economic intelligence and forecasting