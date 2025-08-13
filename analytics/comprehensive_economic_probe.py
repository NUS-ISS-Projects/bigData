#!/usr/bin/env python3
"""
Comprehensive Economic Data Probe
Inspects economic data structure, values, and identifies opportunities for enhancement
"""

import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from analytics.silver_data_connector import SilverLayerConnector, DataSourceConfig

def comprehensive_economic_analysis():
    """Perform comprehensive analysis of economic data for dashboard enhancement"""
    
    print("=== COMPREHENSIVE ECONOMIC DATA PROBE ===")
    
    # Initialize data connector
    config = DataSourceConfig()
    connector = SilverLayerConnector(config)
    
    # Load economic data
    print("\n1. Loading Economic Data...")
    econ_data = connector.load_economic_indicators()
    print(f"   Total records: {len(econ_data):,}")
    print(f"   Columns: {list(econ_data.columns)}")
    
    # Analyze data structure
    print("\n2. Data Structure Analysis:")
    print(f"   Date range: {econ_data['period'].min()} to {econ_data['period'].max()}")
    print(f"   Unique table_ids: {econ_data['table_id'].nunique():,}")
    print(f"   Data quality scores: {econ_data['data_quality_score'].describe()}")
    
    # High-quality data analysis
    high_quality = econ_data[econ_data['data_quality_score'] >= 0.7]
    print(f"\n3. High-Quality Data (score >= 0.7):")
    print(f"   Records: {len(high_quality):,} ({len(high_quality)/len(econ_data)*100:.1f}%)")
    
    # Extract year for recent data analysis
    high_quality_copy = high_quality.copy()
    high_quality_copy['period_year'] = pd.to_numeric(
        high_quality_copy['period'].astype(str).str.extract(r'(\d{4})')[0], 
        errors='coerce'
    )
    
    # Recent data (last 5 years)
    current_year = datetime.now().year
    recent_data = high_quality_copy[
        high_quality_copy['period_year'] >= (current_year - 5)
    ].copy()
    
    print(f"\n4. Recent Data (2020-2025):")
    print(f"   Records: {len(recent_data):,}")
    print(f"   Years: {sorted(recent_data['period_year'].dropna().unique())}")
    
    # Analyze key economic categories
    print("\n5. Key Economic Categories Analysis:")
    
    categories = {
        'GDP': ['GDP', 'GROSS DOMESTIC PRODUCT', 'ECONOMIC GROWTH'],
        'Inflation': ['INFLATION', 'PRICE CHANGE', 'CPI', 'CONSUMER PRICE'],
        'Employment': ['UNEMPLOYMENT', 'EMPLOYMENT', 'LABOUR FORCE', 'JOBS'],
        'Trade': ['EXPORT', 'IMPORT', 'TRADE', 'BALANCE'],
        'Manufacturing': ['MANUFACTURING', 'INDUSTRIAL PRODUCTION', 'FACTORY'],
        'Services': ['SERVICES', 'RETAIL', 'TOURISM', 'HOSPITALITY'],
        'Construction': ['CONSTRUCTION', 'BUILDING', 'HOUSING'],
        'Finance': ['FINANCIAL', 'BANKING', 'MONETARY', 'INTEREST RATE'],
        'Population': ['POPULATION', 'DEMOGRAPHIC', 'BIRTH', 'DEATH'],
        'Education': ['EDUCATION', 'SCHOOL', 'UNIVERSITY', 'STUDENT']
    }
    
    category_analysis = {}
    
    for category, keywords in categories.items():
        # Find matching indicators
        pattern = '|'.join(keywords)
        matches = recent_data[
            recent_data['table_id'].str.contains(pattern, case=False, na=False)
        ]
        
        if not matches.empty:
            # Get latest values for each table_id
            latest = matches.loc[matches.groupby('table_id')['period_year'].idxmax()]
            
            # Filter out extreme values and negatives
            valid_latest = latest[
                (latest['value_numeric'] > 0) & 
                (latest['value_numeric'] < 1e6)  # Remove extreme outliers
            ]
            
            category_analysis[category] = {
                'total_indicators': len(matches['table_id'].unique()),
                'recent_indicators': len(valid_latest),
                'value_range': {
                    'min': float(valid_latest['value_numeric'].min()) if not valid_latest.empty else None,
                    'max': float(valid_latest['value_numeric'].max()) if not valid_latest.empty else None,
                    'mean': float(valid_latest['value_numeric'].mean()) if not valid_latest.empty else None
                },
                'sample_indicators': valid_latest['table_id'].head(3).tolist() if not valid_latest.empty else [],
                'latest_values': [
                    {
                        'table_id': row['table_id'],
                        'value': float(row['value_numeric']),
                        'period': row['period'],
                        'year': int(row['period_year']) if pd.notna(row['period_year']) else None
                    }
                    for _, row in valid_latest.head(5).iterrows()
                ] if not valid_latest.empty else []
            }
        else:
            category_analysis[category] = {
                'total_indicators': 0,
                'recent_indicators': 0,
                'value_range': None,
                'sample_indicators': [],
                'latest_values': []
            }
        
        print(f"   {category}: {category_analysis[category]['recent_indicators']} recent indicators")
    
    # Detailed analysis for top categories
    print("\n6. Detailed Analysis for Top Categories:")
    
    top_categories = sorted(
        [(cat, data['recent_indicators']) for cat, data in category_analysis.items()],
        key=lambda x: x[1],
        reverse=True
    )[:5]
    
    for category, count in top_categories:
        if count > 0:
            data = category_analysis[category]
            print(f"\n   {category} ({count} indicators):")
            print(f"     Value range: {data['value_range']['min']:.2f} - {data['value_range']['max']:.2f}")
            print(f"     Average: {data['value_range']['mean']:.2f}")
            print(f"     Sample indicators: {data['sample_indicators'][:2]}")
            
            # Show latest values
            for val in data['latest_values'][:3]:
                print(f"       - {val['table_id'][:50]}...: {val['value']:.2f} ({val['year']})")
    
    # Export detailed analysis
    print("\n7. Exporting Detailed Analysis...")
    
    export_data = {
        'analysis_timestamp': datetime.now().isoformat(),
        'data_summary': {
            'total_records': len(econ_data),
            'high_quality_records': len(high_quality),
            'recent_records': len(recent_data),
            'date_range': {
                'start': str(econ_data['period'].min()),
                'end': str(econ_data['period'].max())
            }
        },
        'category_analysis': category_analysis,
        'recommendations': {
            'top_categories_for_dashboard': [cat for cat, count in top_categories[:5] if count > 0],
            'data_quality_threshold': 0.7,
            'recent_years_filter': current_year - 5,
            'value_filtering': {
                'min_value': 0,
                'max_value': 1000000,
                'exclude_negatives': True
            }
        }
    }
    
    # Save to JSON file
    output_file = '/home/ngtianxun/bigData_project/analytics/economic_analysis_report.json'
    with open(output_file, 'w') as f:
        json.dump(export_data, f, indent=2, default=str)
    
    print(f"   Analysis saved to: {output_file}")
    
    print("\n=== ANALYSIS COMPLETED ===")
    return export_data

if __name__ == "__main__":
    comprehensive_economic_analysis()