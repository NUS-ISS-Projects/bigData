#!/usr/bin/env python3
"""
Debug script to check GDP data loading for the Economic Indicators dashboard
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
from silver_data_connector import SilverLayerConnector

def debug_gdp_data():
    print("=== GDP Data Debug Analysis ===")
    
    # Initialize silver connector
    silver_connector = SilverLayerConnector()
    
    try:
        # Load economic indicators data
        print("\n1. Loading economic indicators data...")
        gdp_df = silver_connector.load_economic_indicators()
        print(f"Total economic records loaded: {len(gdp_df)}")
        
        if gdp_df.empty:
            print("ERROR: No economic data loaded!")
            return
        
        # Show data structure
        print("\n2. Data structure:")
        print(f"Columns: {list(gdp_df.columns)}")
        
        # Check for GDP data by examining table_id and series_id
        print("\n3. Checking for GDP data in table_id and series_id...")
        
        # Show unique table_id values
        if 'table_id' in gdp_df.columns:
            unique_table_ids = gdp_df['table_id'].unique()
            print(f"Unique table_id values ({len(unique_table_ids)}):")
            for tid in sorted(unique_table_ids)[:20]:  # Show first 20
                count = len(gdp_df[gdp_df['table_id'] == tid])
                print(f"  - {tid}: {count} records")
            if len(unique_table_ids) > 20:
                print(f"  ... and {len(unique_table_ids) - 20} more")
        
        # Look for GDP-related table_ids
        gdp_tables = gdp_df[gdp_df['table_id'].str.contains('GDP|GROSS', na=False, case=False)]
        print(f"\n4. GDP-related table_ids found: {len(gdp_tables)}")
        
        if not gdp_tables.empty:
            print("GDP table_ids:")
            for tid in gdp_tables['table_id'].unique():
                count = len(gdp_tables[gdp_tables['table_id'] == tid])
                units = gdp_tables[gdp_tables['table_id'] == tid]['unit'].unique()
                print(f"  - {tid}: {count} records, units: {units}")
        
        # Look for GDP in series_id
        gdp_series = gdp_df[gdp_df['series_id'].str.contains('GDP|GROSS', na=False, case=False)]
        print(f"\n5. GDP-related series_id found: {len(gdp_series)}")
        
        if not gdp_series.empty:
            print("GDP series_ids (first 10):")
            for sid in gdp_series['series_id'].unique()[:10]:
                count = len(gdp_series[gdp_series['series_id'] == sid])
                units = gdp_series[gdp_series['series_id'] == sid]['unit'].unique()
                print(f"  - {sid}: {count} records, units: {units}")
        
        # Check for Million Dollars unit data
        million_dollar_data = gdp_df[gdp_df['unit'] == 'Million Dollars']
        print(f"\n6. Million Dollars unit data: {len(million_dollar_data)} records")
        
        if not million_dollar_data.empty:
            print("Sample Million Dollars data:")
            sample_data = million_dollar_data[['table_id', 'series_id', 'period', 'value_numeric', 'unit']].head(10)
            print(sample_data)
        
        # Try alternative filtering approach
        print("\n7. Trying alternative GDP filtering...")
        
        # Filter by unit and look for GDP-like patterns
        potential_gdp = gdp_df[
            (gdp_df['unit'].isin(['Million Dollars', 'Millions Of Singapore Dollars'])) &
            (gdp_df['series_id'].str.contains('GDP|GROSS|DOMESTIC|PRODUCT', na=False, case=False))
        ]
        
        # Test the exact same filtering as in the dashboard
        gdp_filtered = gdp_df[
            (gdp_df['table_id'].str.contains('GROSS DOMESTIC PRODUCT AT CURRENT PRICES', na=False)) &
            (gdp_df['unit'] == 'Million Dollars')
        ].copy()
        
        print(f"\nExact dashboard filtering test: {len(gdp_filtered)} records")
        if not gdp_filtered.empty:
            print("Dashboard filter results:")
            for sid in gdp_filtered['series_id'].unique()[:5]:
                count = len(gdp_filtered[gdp_filtered['series_id'] == sid])
                print(f"  - {sid}: {count} records")
        
        print(f"Potential GDP data found: {len(potential_gdp)}")
        
        if not potential_gdp.empty:
            print("\nPotential GDP series:")
            for sid in potential_gdp['series_id'].unique()[:10]:
                count = len(potential_gdp[potential_gdp['series_id'] == sid])
                period_range = f"{potential_gdp[potential_gdp['series_id'] == sid]['period'].min()} to {potential_gdp[potential_gdp['series_id'] == sid]['period'].max()}"
                print(f"  - {sid}: {count} records, period: {period_range}")
            
            # Try to create GDP growth chart with this data
            print("\n8. Testing GDP growth calculation...")
            test_series = potential_gdp['series_id'].iloc[0]
            test_data = potential_gdp[potential_gdp['series_id'] == test_series].copy()
            
            # Convert period to datetime and extract year
            test_data['year'] = pd.to_datetime(test_data['period']).dt.year
            
            # Group by year and sum values
            annual_data = test_data.groupby('year')['value_numeric'].sum().reset_index()
            annual_data = annual_data.sort_values('year')
            
            # Calculate growth rates
            annual_data['gdp_growth'] = annual_data['value_numeric'].pct_change() * 100
            growth_data = annual_data.dropna()
            
            print(f"Test series: {test_series}")
            print(f"Annual data points: {len(annual_data)}")
            print(f"Growth data points: {len(growth_data)}")
            
            if not growth_data.empty:
                print(f"Average growth: {growth_data['gdp_growth'].mean():.2f}%")
                print("Recent data:")
                print(growth_data[['year', 'value_numeric', 'gdp_growth']].tail(5))
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        silver_connector.close_connection()

if __name__ == "__main__":
    debug_gdp_data()