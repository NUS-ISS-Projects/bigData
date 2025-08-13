#!/usr/bin/env python3
"""
Economic Indicators Data Probe
Analyzes the structure and content of economic indicators data
to improve dashboard visualization and understanding.
"""

import sys
import pandas as pd
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from analytics.silver_data_connector import SilverLayerConnector

def analyze_economic_data():
    """Comprehensive analysis of economic indicators data"""
    print("=== Economic Indicators Data Analysis ===")
    
    # Load data
    connector = SilverLayerConnector()
    data = connector.load_economic_indicators()
    
    print(f"\n📊 Dataset Overview:")
    print(f"Total records: {len(data):,}")
    print(f"Columns: {list(data.columns)}")
    print(f"Date range: {data['period'].min()} to {data['period'].max()}")
    
    print(f"\n📈 Table ID Analysis:")
    table_counts = data['table_id'].value_counts()
    print(f"Unique table IDs: {len(table_counts)}")
    print("\nTop 10 most frequent table IDs:")
    for i, (table_id, count) in enumerate(table_counts.head(10).items(), 1):
        print(f"{i:2d}. {table_id[:80]}{'...' if len(table_id) > 80 else ''} ({count:,} records)")
    
    print(f"\n💰 Value Analysis:")
    numeric_data = data[data['is_valid_numeric'] == True]
    print(f"Valid numeric records: {len(numeric_data):,} ({len(numeric_data)/len(data)*100:.1f}%)")
    print(f"Value range: {numeric_data['value_numeric'].min():,.2f} to {numeric_data['value_numeric'].max():,.2f}")
    print(f"Mean value: {numeric_data['value_numeric'].mean():,.2f}")
    print(f"Median value: {numeric_data['value_numeric'].median():,.2f}")
    
    print(f"\n📅 Time Period Analysis:")
    period_analysis = data.groupby('period_year').size().sort_index()
    print(f"Years covered: {period_analysis.index.min()} to {period_analysis.index.max()}")
    print("Records per year (last 5 years):")
    for year, count in period_analysis.tail(5).items():
        print(f"  {year}: {count:,} records")
    
    print(f"\n🏷️ Data Quality Analysis:")
    quality_stats = data['data_quality_score'].describe()
    print(f"Quality score range: {quality_stats['min']:.2f} to {quality_stats['max']:.2f}")
    print(f"Average quality score: {quality_stats['mean']:.2f}")
    
    print(f"\n🔍 Sample Records for Key Indicators:")
    # Look for common economic indicators
    key_indicators = [
        'GDP', 'CONSUMER PRICE INDEX', 'UNEMPLOYMENT', 'INFLATION', 
        'GROSS DOMESTIC PRODUCT', 'CPI'
    ]
    
    for indicator in key_indicators:
        matching_tables = data[data['table_id'].str.contains(indicator, case=False, na=False)]
        if not matching_tables.empty:
            print(f"\n{indicator} related data:")
            latest_data = matching_tables.sort_values('period').tail(3)
            for _, row in latest_data.iterrows():
                print(f"  {row['period']}: {row['value_original']} ({row['table_id'][:60]}...)")
    
    print(f"\n🎯 Recommendations for Dashboard Improvement:")
    print("1. Group similar indicators (GDP, CPI, etc.) for better visualization")
    print("2. Use latest available data points for each indicator")
    print("3. Add time series charts for trending analysis")
    print("4. Filter by data quality score for more reliable indicators")
    print("5. Create categorical groupings for better organization")
    
    return data

if __name__ == "__main__":
    analyze_economic_data()