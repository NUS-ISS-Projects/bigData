#!/usr/bin/env python3
"""
Debug script to examine GDP data granularity and periods available
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
from silver_data_connector import SilverLayerConnector

def main():
    print("=== GDP Data Granularity Analysis ===")
    
    # Load GDP data from silver layer
    silver_connector = SilverLayerConnector()
    gdp_df = silver_connector.load_economic_indicators()
    
    print(f"Total economic indicators records: {len(gdp_df)}")
    
    # Filter for GDP at current prices data
    gdp_filtered = gdp_df[
        (gdp_df['table_id'].str.contains('GROSS DOMESTIC PRODUCT AT CURRENT PRICES', na=False)) &
        (gdp_df['unit'] == 'Million Dollars')
    ].copy()
    
    print(f"GDP records found: {len(gdp_filtered)}")
    
    if not gdp_filtered.empty:
        print("\n=== Period Analysis ===")
        print("Sample periods:")
        print(gdp_filtered['period'].head(10).tolist())
        
        print("\nUnique periods (first 20):")
        unique_periods = sorted(gdp_filtered['period'].unique())
        print(unique_periods[:20])
        
        print(f"\nTotal unique periods: {len(unique_periods)}")
        print(f"Period range: {min(unique_periods)} to {max(unique_periods)}")
        
        # Analyze period patterns
        print("\n=== Period Pattern Analysis ===")
        period_patterns = {}
        for period in unique_periods:
            if 'Q' in str(period):
                period_patterns['Quarterly'] = period_patterns.get('Quarterly', 0) + 1
            elif len(str(period)) == 4 and str(period).isdigit():
                period_patterns['Annual'] = period_patterns.get('Annual', 0) + 1
            elif '-' in str(period) and len(str(period)) >= 7:
                period_patterns['Monthly'] = period_patterns.get('Monthly', 0) + 1
            else:
                period_patterns['Other'] = period_patterns.get('Other', 0) + 1
        
        print("Period type distribution:")
        for pattern, count in period_patterns.items():
            print(f"  {pattern}: {count} periods")
        
        # Show sample data for each pattern
        print("\n=== Sample Data by Period Type ===")
        
        # Quarterly data
        quarterly_data = gdp_filtered[gdp_filtered['period'].str.contains('Q', na=False)]
        if not quarterly_data.empty:
            print(f"\nQuarterly data ({len(quarterly_data)} records):")
            print(quarterly_data[['period', 'value_numeric', 'series_id']].head(10))
        
        # Annual data
        annual_data = gdp_filtered[gdp_filtered['period'].str.len() == 4]
        if not annual_data.empty:
            print(f"\nAnnual data ({len(annual_data)} records):")
            print(annual_data[['period', 'value_numeric', 'series_id']].head(10))
        
        # Monthly data
        monthly_data = gdp_filtered[gdp_filtered['period'].str.contains('-', na=False) & (gdp_filtered['period'].str.len() >= 7)]
        if not monthly_data.empty:
            print(f"\nMonthly data ({len(monthly_data)} records):")
            print(monthly_data[['period', 'value_numeric', 'series_id']].head(10))
        
        # Series ID analysis
        print("\n=== Series ID Analysis ===")
        series_counts = gdp_filtered['series_id'].value_counts()
        print("Top series IDs:")
        print(series_counts.head(10))
        
        # Show data for the most common series
        if not series_counts.empty:
            top_series = series_counts.index[0]
            top_series_data = gdp_filtered[gdp_filtered['series_id'] == top_series].sort_values('period')
            print(f"\nData for top series ({top_series}):")
            print(top_series_data[['period', 'value_numeric']].head(20))
    
    else:
        print("No GDP data found!")

if __name__ == "__main__":
    main()