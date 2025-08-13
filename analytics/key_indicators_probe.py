#!/usr/bin/env python3
"""
Key Economic Indicators Data Probe
Investigates the data structure and values for key economic performance metrics
"""

import sys
sys.path.append('/home/ngtianxun/bigData_project/analytics')
from silver_data_connector import SilverLayerConnector
import pandas as pd
import json

def main():
    print('=== KEY ECONOMIC INDICATORS DATA PROBE ===')
    
    # Initialize connector and load data
    connector = SilverLayerConnector()
    economic_data = connector.load_economic_indicators()
    
    print(f'\nTotal economic records: {len(economic_data)}')
    print(f'Columns: {list(economic_data.columns)}')
    
    # Filter for high quality data
    quality_data = economic_data[economic_data['data_quality_score'] >= 0.8]
    print(f'High quality records: {len(quality_data)}')
    
    # Check key indicators mapping used in the chart
    key_indicators_map = {
        'GDP': ['GDP', 'Gross Domestic Product'],
        'CPI': ['CPI', 'Consumer Price Index'],
        'Unemployment': ['Unemployment', 'Labour Force'],
        'Inflation': ['Inflation', 'Price']
    }
    
    print('\n=== KEY INDICATORS ANALYSIS ===')
    chart_data = []
    
    for short_name, keywords in key_indicators_map.items():
        matching_records = quality_data[
            quality_data['table_id'].str.contains('|'.join(keywords), case=False, na=False)
        ]
        print(f'\n{short_name} indicators:')
        print(f'  Found {len(matching_records)} records')
        
        if len(matching_records) > 0:
            # Get latest values by table_id (this is what the chart uses)
            latest_values = matching_records.groupby('table_id')['value_numeric'].last()
            print(f'  Latest values by table_id:')
            
            for table_id, value in latest_values.head(5).items():
                print(f'    {table_id}: {value:,.2f}')
                
                # Get the latest period for this table_id
                latest_period = matching_records[matching_records['table_id'] == table_id]['period'].iloc[-1]
                chart_label = f'{short_name} ({latest_period})'
                chart_data.append({
                    'label': chart_label,
                    'value': value,
                    'table_id': table_id,
                    'period': latest_period
                })
            
            # Check value ranges
            print(f'  Value range: {matching_records["value_numeric"].min():,.2f} to {matching_records["value_numeric"].max():,.2f}')
            
            # Check periods
            periods = matching_records['period'].unique()
            print(f'  Time periods: {sorted(periods)[-5:]} (latest 5)')
    
    print('\n=== CHART DATA STRUCTURE ===')
    print('Data that would be used in the horizontal bar chart:')
    for item in chart_data[:10]:  # Show first 10 items
        print(f"  {item['label']}: {item['value']:,.2f}")
    
    # Check for potential issues
    print('\n=== POTENTIAL ISSUES ===')
    
    # Check for extreme values
    values = [item['value'] for item in chart_data]
    if values:
        min_val, max_val = min(values), max(values)
        print(f'Value range in chart: {min_val:,.2f} to {max_val:,.2f}')
        
        # Check for scale issues
        if max_val > 1000000:
            print('WARNING: Very large values detected - may need scaling')
        
        # Check for negative values
        negative_count = sum(1 for v in values if v < 0)
        if negative_count > 0:
            print(f'WARNING: {negative_count} negative values detected')
        
        # Check for zero values
        zero_count = sum(1 for v in values if v == 0)
        if zero_count > 0:
            print(f'WARNING: {zero_count} zero values detected')
    
    # Sample some actual table_ids to understand the data better
    print('\n=== SAMPLE TABLE IDS ===')
    sample_tables = quality_data['table_id'].unique()[:10]
    for table_id in sample_tables:
        sample_data = quality_data[quality_data['table_id'] == table_id]
        latest_value = sample_data['value_numeric'].iloc[-1]
        latest_period = sample_data['period'].iloc[-1]
        print(f'{table_id}: {latest_value:,.2f} ({latest_period})')
    
    print('\n=== PROBE COMPLETED ===')

if __name__ == '__main__':
    main()