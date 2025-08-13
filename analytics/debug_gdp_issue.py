#!/usr/bin/env python3
"""
Debug script to investigate the "Insufficient GDP data for growth calculation" issue
"""

import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from silver_data_connector import SilverLayerConnector

def debug_gdp_data():
    """Debug GDP data filtering and processing"""
    print("=== GDP Data Debug Analysis ===")
    
    try:
        # Load GDP data from silver layer
        silver_connector = SilverLayerConnector()
        
        # Load economic indicators data from silver layer
        gdp_df = silver_connector.load_economic_indicators()
        print(f"Total economic indicators loaded: {len(gdp_df)}")
        
        # Check unique table_ids
        print("\n=== Available Table IDs ===")
        table_ids = gdp_df['table_id'].unique()
        for table_id in sorted(table_ids):
            count = len(gdp_df[gdp_df['table_id'] == table_id])
            print(f"  {table_id}: {count} records")
        
        # Filter for GDP at current prices data
        print("\n=== GDP Filtering Step 1: Table ID Filter ===")
        gdp_filtered = gdp_df[
            (gdp_df['table_id'].str.contains('GROSS DOMESTIC PRODUCT AT CURRENT PRICES', na=False)) &
            (gdp_df['unit'] == 'Million Dollars')
        ].copy()
        print(f"Records after GDP table filter: {len(gdp_filtered)}")
        
        if len(gdp_filtered) == 0:
            print("\n=== Alternative GDP Table ID Search ===")
            # Look for any GDP-related table IDs
            gdp_tables = gdp_df[gdp_df['table_id'].str.contains('GDP|GROSS DOMESTIC PRODUCT', na=False, case=False)]
            if len(gdp_tables) > 0:
                print("Found GDP-related tables:")
                for table_id in gdp_tables['table_id'].unique():
                    count = len(gdp_tables[gdp_tables['table_id'] == table_id])
                    units = gdp_tables[gdp_tables['table_id'] == table_id]['unit'].unique()
                    print(f"  {table_id}: {count} records, units: {units}")
            else:
                print("No GDP-related tables found!")
                
            # Check available units
            print("\n=== Available Units ===")
            units = gdp_df['unit'].unique()
            for unit in sorted(units):
                count = len(gdp_df[gdp_df['unit'] == unit])
                print(f"  {unit}: {count} records")
            return
        
        # Check units in filtered data
        print("\n=== Units in GDP filtered data ===")
        units = gdp_filtered['unit'].unique()
        for unit in units:
            count = len(gdp_filtered[gdp_filtered['unit'] == unit])
            print(f"  {unit}: {count} records")
        
        # Check periods
        print("\n=== Period Analysis ===")
        periods = gdp_filtered['period'].unique()
        print(f"Total unique periods: {len(periods)}")
        print(f"Sample periods: {sorted(periods)[:10]}")
        
        # Filter for quarterly data
        print("\n=== GDP Filtering Step 2: Quarterly Data ===")
        quarterly_data = gdp_filtered[gdp_filtered['period'].str.contains('Q', na=False)].copy()
        print(f"Records with quarterly data: {len(quarterly_data)}")
        
        if len(quarterly_data) > 0:
            print("\n=== Quarterly Data Analysis ===")
            # Check series IDs
            series_counts = quarterly_data['series_id'].value_counts()
            print(f"Series ID counts: {series_counts}")
            
            # Use the most common series
            main_series = series_counts.index[0]
            gdp_quarterly = quarterly_data[quarterly_data['series_id'] == main_series].copy()
            print(f"Records for main series {main_series}: {len(gdp_quarterly)}")
            
            # Sort by period
            gdp_quarterly = gdp_quarterly.sort_values('period')
            print(f"Period range: {gdp_quarterly['period'].min()} to {gdp_quarterly['period'].max()}")
            
            # Check data types and values before calculation
            print("\n=== Data Quality Check ===")
            print(f"value_numeric dtype: {gdp_quarterly['value_numeric'].dtype}")
            print(f"Non-null values: {gdp_quarterly['value_numeric'].notna().sum()}")
            print(f"Null values: {gdp_quarterly['value_numeric'].isna().sum()}")
            print(f"Sample values: {gdp_quarterly['value_numeric'].head(10).tolist()}")
            
            # Check if values are actually numeric
            try:
                numeric_values = pd.to_numeric(gdp_quarterly['value_numeric'], errors='coerce')
                print(f"Values after numeric conversion: {numeric_values.notna().sum()} valid, {numeric_values.isna().sum()} invalid")
                gdp_quarterly['value_numeric'] = numeric_values
            except Exception as e:
                print(f"Error converting to numeric: {e}")
            
            # Calculate growth rate
            print("\n=== Growth Calculation ===")
            gdp_quarterly['gdp_growth'] = gdp_quarterly['value_numeric'].pct_change() * 100
            print(f"Growth values calculated: {gdp_quarterly['gdp_growth'].notna().sum()}")
            print(f"Sample growth values: {gdp_quarterly['gdp_growth'].head(10).tolist()}")
            
            # Check all columns for NaN values
            print("\n=== Column NaN Analysis ===")
            for col in gdp_quarterly.columns:
                nan_count = gdp_quarterly[col].isna().sum()
                if nan_count > 0:
                    print(f"{col}: {nan_count} NaN values")
            
            # Use specific dropna for growth calculation
            gdp_growth_data = gdp_quarterly.dropna(subset=['gdp_growth'])
            print(f"Records after growth calculation (dropna on gdp_growth only): {len(gdp_growth_data)}")
            
            if len(gdp_growth_data) > 0:
                print("\n=== Growth Data Sample ===")
                print(gdp_growth_data[['period', 'value_numeric', 'gdp_growth']].head(10))
                print("\nSUCCESS: GDP growth data is available!")
            else:
                print("\nERROR: No growth data after calculation!")
                print("\n=== Debugging dropna() ===")
                print(f"Records before dropna: {len(gdp_quarterly)}")
                print(f"Records with non-null value_numeric: {gdp_quarterly['value_numeric'].notna().sum()}")
                print(f"Records with non-null gdp_growth: {gdp_quarterly['gdp_growth'].notna().sum()}")
                print(f"Records with both non-null: {(gdp_quarterly['value_numeric'].notna() & gdp_quarterly['gdp_growth'].notna()).sum()}")
        else:
            print("\n=== Fallback to Annual Data ===")
            # Try annual data
            try:
                gdp_filtered['year'] = pd.to_datetime(gdp_filtered['period']).dt.year
                gdp_annual = gdp_filtered.groupby('year')['value_numeric'].sum().reset_index()
                gdp_annual = gdp_annual.sort_values('year')
                gdp_annual['gdp_growth'] = gdp_annual['value_numeric'].pct_change() * 100
                gdp_growth_data = gdp_annual.dropna()
                gdp_growth_data['period'] = gdp_growth_data['year'].astype(str)
                print(f"Annual growth data records: {len(gdp_growth_data)}")
                
                if len(gdp_growth_data) > 0:
                    print("\n=== Annual Growth Data Sample ===")
                    print(gdp_growth_data[['year', 'value_numeric', 'gdp_growth']].head(10))
                    print("\nSUCCESS: Annual GDP growth data is available!")
                else:
                    print("\nERROR: No annual growth data available!")
            except Exception as e:
                print(f"\nERROR in annual data processing: {e}")
        
    except Exception as e:
        print(f"Error loading GDP data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_gdp_data()