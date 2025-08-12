#!/usr/bin/env python3
"""
Detailed Economic Indicator Probe
Investigates why certain categories aren't appearing in the Key Economic Performance Metrics
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from analytics.silver_data_connector import SilverLayerConnector, DataSourceConfig

def detailed_indicator_investigation():
    """Investigate indicator matching and filtering logic"""
    
    print("=== DETAILED INDICATOR INVESTIGATION ===")
    
    # Initialize data connector
    config = DataSourceConfig()
    connector = SilverLayerConnector(config)
    
    # Load economic data
    econ_data = connector.load_economic_indicators()
    print(f"Total economic records: {len(econ_data):,}")
    
    # Filter for high-quality data
    quality_data = econ_data[econ_data['data_quality_score'] >= 0.9]
    print(f"High-quality records (score >= 0.9): {len(quality_data):,}")
    
    # Define the same categories as in the enhanced chart
    key_categories = {
        'GDP Growth': ['GDP', 'GROSS DOMESTIC PRODUCT'],
        'Consumer Prices': ['CPI', 'CONSUMER PRICE INDEX'],
        'Employment': ['UNEMPLOYMENT', 'EMPLOYMENT', 'LABOUR FORCE'],
        'Inflation': ['INFLATION', 'PRICE CHANGE', 'PERCENT CHANGE'],
        'Manufacturing': ['MANUFACTURING', 'INDUSTRIAL PRODUCTION', 'FACTORY'],
        'Services': ['SERVICES', 'EXPORTS OF SERVICES', 'IMPORTS OF SERVICES'],
        'Trade': ['EXPORT', 'IMPORT', 'TRADE', 'TRADING PARTNER'],
        'Construction': ['CONSTRUCTION', 'BUILDING', 'HOUSING'],
        'Finance': ['FINANCIAL', 'BANKING', 'MONETARY']
    }
    
    # Extract year for recent data analysis
    quality_data_copy = quality_data.copy()
    quality_data_copy['period_year'] = pd.to_numeric(
        quality_data_copy['period'].astype(str).str.extract(r'(\d{4})')[0], 
        errors='coerce'
    )
    
    # Recent data (last 5 years)
    current_year = datetime.now().year
    recent_data = quality_data_copy[
        quality_data_copy['period_year'] >= (current_year - 5)
    ].copy()
    
    print(f"\nRecent data (2020-2025): {len(recent_data):,} records")
    
    # Investigate each category
    print("\n=== CATEGORY INVESTIGATION ===")
    
    for category, keywords in key_categories.items():
        print(f"\n{category}:")
        print(f"  Keywords: {keywords}")
        
        # Find matching indicators in recent data
        pattern = '|'.join(keywords)
        matches = recent_data[
            recent_data['table_id'].str.contains(pattern, case=False, na=False)
        ]
        
        print(f"  Total matches in recent data: {len(matches)}")
        
        if not matches.empty:
            # Show unique table_ids
            unique_tables = matches['table_id'].unique()
            print(f"  Unique table_ids: {len(unique_tables)}")
            
            # Show sample table_ids
            print(f"  Sample table_ids:")
            for i, table_id in enumerate(unique_tables[:3]):
                print(f"    {i+1}. {table_id}")
            
            # Get latest values for each table_id
            latest = matches.loc[matches.groupby('table_id')['period_year'].idxmax()]
            print(f"  Latest indicators: {len(latest)}")
            
            # Check value filtering
            valid_latest = latest[
                (latest['value_numeric'] > 0) & 
                (latest['value_numeric'] < 1000000)
            ]
            print(f"  Valid indicators (positive, < 1M): {len(valid_latest)}")
            
            if not valid_latest.empty:
                # Show value ranges
                values = valid_latest['value_numeric']
                print(f"  Value range: {values.min():.2f} to {values.max():.2f}")
                
                # Check for annual vs quarterly preference
                annual_data = valid_latest[
                    valid_latest['table_id'].str.contains('ANNUAL', case=False, na=False)
                ]
                print(f"  Annual indicators: {len(annual_data)}")
                
                if not annual_data.empty:
                    best_indicator = annual_data.iloc[0]
                    print(f"  Best annual indicator: {best_indicator['table_id'][:60]}...")
                    print(f"    Value: {best_indicator['value_numeric']:.2f}")
                    print(f"    Period: {best_indicator['period']}")
                elif not valid_latest.empty:
                    best_indicator = valid_latest.iloc[0]
                    print(f"  Best indicator: {best_indicator['table_id'][:60]}...")
                    print(f"    Value: {best_indicator['value_numeric']:.2f}")
                    print(f"    Period: {best_indicator['period']}")
            else:
                print(f"  No valid indicators found (all filtered out)")
                if not latest.empty:
                    print(f"  Filtered out reasons:")
                    negative_count = (latest['value_numeric'] <= 0).sum()
                    extreme_count = (latest['value_numeric'] >= 1000000).sum()
                    print(f"    - Negative/zero values: {negative_count}")
                    print(f"    - Extreme values (>= 1M): {extreme_count}")
        else:
            print(f"  No matches found for keywords: {keywords}")
            
            # Try broader search
            broader_matches = recent_data[
                recent_data['table_id'].str.contains(keywords[0], case=False, na=False)
            ]
            if not broader_matches.empty:
                print(f"  Broader search for '{keywords[0]}': {len(broader_matches)} matches")
                sample_tables = broader_matches['table_id'].unique()[:3]
                for i, table in enumerate(sample_tables):
                    print(f"    {i+1}. {table}")
    
    print("\n=== INVESTIGATION COMPLETED ===")

if __name__ == "__main__":
    detailed_indicator_investigation()