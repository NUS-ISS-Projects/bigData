#!/usr/bin/env python3

from enhanced_visual_intelligence import EnhancedVisualIntelligencePlatform
from silver_data_connector import SilverLayerConnector, DataSourceConfig
from llm_config import get_default_config
import json

def main():
    # Initialize components
    config = DataSourceConfig()
    llm_config = get_default_config()
    platform = EnhancedVisualIntelligencePlatform(config, llm_config)
    
    # Generate report
    report = platform.generate_enhanced_visual_report()
    
    # Get business formation charts
    bf_charts = report.get('visualizations', {}).get('business_formation', {})
    
    print('Available Business Formation Charts:')
    print('=' * 50)
    
    for name, chart in bf_charts.items():
        print(f'\nChart: {name}')
        print(f'Title: {chart.get("title", "No title")}')
        print(f'Type: {chart.get("type", "Unknown")}')
        print(f'Description: {chart.get("description", "No description")}')
        
        data = chart.get('data', {})
        print(f'Data keys: {list(data.keys())}')
        
        # Show sample data
        for key, value in data.items():
            if isinstance(value, list):
                print(f'  {key}: {len(value)} items - Sample: {value[:3] if len(value) > 3 else value}')
            else:
                print(f'  {key}: {value}')
        print('-' * 30)

if __name__ == '__main__':
    main()