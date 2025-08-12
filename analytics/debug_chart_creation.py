#!/usr/bin/env python3

import sys
sys.path.append('/home/ngtianxun/bigData_project')

from analytics.silver_data_connector import SilverLayerConnector, DataSourceConfig
from analytics.enhanced_visual_intelligence import VisualEconomicAnalyzer
import json

# Initialize connector and analyzer
connector = SilverLayerConnector()
analyzer = VisualEconomicAnalyzer(connector)

# Load ACRA data
print("Loading ACRA data...")
acra_data = connector.load_acra_companies()
print(f"Loaded {len(acra_data)} ACRA records")

# Check entity_status column
if 'entity_status' in acra_data.columns:
    status_counts = acra_data['entity_status'].value_counts()
    print(f"Entity status counts: {status_counts.to_dict()}")
    
    # Create business formation charts
    print("\nCreating business formation charts...")
    charts = analyzer._create_business_formation_charts(acra_data)
    
    # Check company_status chart
    if 'company_status' in charts:
        company_status_chart = charts['company_status']
        print(f"\nCompany status chart data:")
        print(json.dumps(company_status_chart, indent=2))
    else:
        print("\nNo company_status chart found!")
        print(f"Available charts: {list(charts.keys())}")
else:
    print("No entity_status column found!")
    print(f"Available columns: {list(acra_data.columns)}")