#!/usr/bin/env python3

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
from analytics.silver_data_connector import SilverLayerConnector, DataSourceConfig

def debug_economic_trends_data():
    """Debug the economic trends chart data filtering issues"""
    
    print("=== ECONOMIC TRENDS CHART DEBUG ===")
    
    # Initialize connector
    config = DataSourceConfig()
    connector = SilverLayerConnector(config)
    
    try:
        # Load economic data
        econ_data = connector.load_economic_indicators()
        print(f"Total economic records: {len(econ_data):,}")
        
        # Filter for high-quality data (same as in the chart)
        quality_data = econ_data[econ_data['data_quality_score'] >= 0.9]
        print(f"High-quality records: {len(quality_data):,}")
        
        print("\n=== GDP DATA ANALYSIS ===")
        
        # Current restrictive GDP filter from enhanced_visual_intelligence.py
        print("\n1. Current restrictive GDP filter:")
        print("   Filter: 'GROSS DOMESTIC PRODUCT.*CURRENT PRICES.*QUARTERLY'")
        
        restrictive_gdp = quality_data[
            quality_data['table_id'].str.contains('GROSS DOMESTIC PRODUCT.*CURRENT PRICES.*QUARTERLY', case=False, na=False)
        ]
        print(f"   Results: {len(restrictive_gdp)} records")
        
        if not restrictive_gdp.empty:
            print("   Table IDs found:")
            for table_id in restrictive_gdp['table_id'].unique():
                count = len(restrictive_gdp[restrictive_gdp['table_id'] == table_id])
                periods = restrictive_gdp[restrictive_gdp['table_id'] == table_id]['period'].unique()
                print(f"     - {table_id}: {count} records, periods: {len(periods)}")
                if len(periods) > 0:
                    print(f"       Period range: {min(periods)} to {max(periods)}")
        
        # Better GDP filter
        print("\n2. Better GDP filter:")
        print("   Filter: 'GROSS DOMESTIC PRODUCT.*CURRENT PRICES' (without QUARTERLY requirement)")
        
        better_gdp = quality_data[
            quality_data['table_id'].str.contains('GROSS DOMESTIC PRODUCT.*CURRENT PRICES', case=False, na=False)
        ]
        print(f"   Results: {len(better_gdp)} records")
        
        if not better_gdp.empty:
            print("   Table IDs found:")
            for table_id in better_gdp['table_id'].unique()[:5]:  # Show first 5
                count = len(better_gdp[better_gdp['table_id'] == table_id])
                periods = better_gdp[better_gdp['table_id'] == table_id]['period'].unique()
                print(f"     - {table_id}: {count} records, periods: {len(periods)}")
                if len(periods) > 0:
                    print(f"       Period range: {min(periods)} to {max(periods)}")
        
        # Check for quarterly data specifically
        print("\n3. Quarterly GDP data analysis:")
        quarterly_gdp = better_gdp[better_gdp['period'].str.contains('Q', na=False)]
        print(f"   Quarterly records: {len(quarterly_gdp)}")
        
        if not quarterly_gdp.empty:
            print("   Recent quarterly periods:")
            recent_quarters = sorted(quarterly_gdp['period'].unique())[-10:]
            for period in recent_quarters:
                count = len(quarterly_gdp[quarterly_gdp['period'] == period])
                print(f"     - {period}: {count} records")
        
        print("\n=== CPI DATA ANALYSIS ===")
        
        # Current restrictive CPI filter
        print("\n1. Current restrictive CPI filter:")
        print("   Filter: 'CONSUMER PRICE INDEX.*2024.*BASE'")
        
        restrictive_cpi = quality_data[
            quality_data['table_id'].str.contains('CONSUMER PRICE INDEX.*2024.*BASE', case=False, na=False)
        ]
        print(f"   Results: {len(restrictive_cpi)} records")
        
        if not restrictive_cpi.empty:
            print("   Table IDs found:")
            for table_id in restrictive_cpi['table_id'].unique():
                count = len(restrictive_cpi[restrictive_cpi['table_id'] == table_id])
                periods = restrictive_cpi[restrictive_cpi['table_id'] == table_id]['period'].unique()
                print(f"     - {table_id}: {count} records, periods: {len(periods)}")
                if len(periods) > 0:
                    print(f"       Period range: {min(periods)} to {max(periods)}")
        
        # Better CPI filter
        print("\n2. Better CPI filter:")
        print("   Filter: 'CONSUMER PRICE INDEX' (without year restriction)")
        
        better_cpi = quality_data[
            quality_data['table_id'].str.contains('CONSUMER PRICE INDEX', case=False, na=False)
        ]
        print(f"   Results: {len(better_cpi)} records")
        
        if not better_cpi.empty:
            print("   Table IDs found (first 5):")
            for table_id in better_cpi['table_id'].unique()[:5]:
                count = len(better_cpi[better_cpi['table_id'] == table_id])
                periods = better_cpi[better_cpi['table_id'] == table_id]['period'].unique()
                print(f"     - {table_id}: {count} records, periods: {len(periods)}")
                if len(periods) > 0:
                    print(f"       Period range: {min(periods)} to {max(periods)}")
        
        print("\n=== RECOMMENDATIONS ===")
        print("\n1. GDP and CPI should be separated into different charts")
        print("   - GDP shows absolute values in millions of dollars")
        print("   - CPI shows index values (around 100)")
        print("   - Different scales make combined visualization misleading")
        
        print("\n2. Remove overly restrictive filters")
        print("   - GDP: Remove 'QUARTERLY' requirement from table_id filter")
        print("   - CPI: Remove '2024.*BASE' requirement")
        
        print("\n3. Increase data range")
        print("   - Current: Only last 20 periods")
        print("   - Recommended: Show more historical data for better trends")
        
        print("\n4. Use appropriate data for each chart")
        print("   - GDP: Use quarterly data if available, fallback to annual")
        print("   - CPI: Use monthly data if available for better granularity")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        connector.close_connection()

if __name__ == "__main__":
    debug_economic_trends_data()