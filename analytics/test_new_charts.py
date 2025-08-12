#!/usr/bin/env python3
"""
Test script to verify the new GDP and CPI chart structure
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from enhanced_visual_intelligence import EnhancedVisualIntelligencePlatform
from silver_data_connector import SilverLayerConnector
import json

def test_new_chart_structure():
    print("🔍 Testing New GDP and CPI Chart Structure")
    print("=" * 50)
    
    try:
        # Initialize components
        silver_connector = SilverLayerConnector()
        visual_platform = EnhancedVisualIntelligencePlatform()
        
        # Load economic indicators data
        print("📊 Loading economic indicators data...")
        data = silver_connector.load_economic_indicators()
        print(f"✅ Loaded {len(data)} records")
        
        # Generate charts with new structure
        print("\n🎨 Generating charts with new structure...")
        charts = visual_platform.visual_analyzer._create_economic_indicators_charts(data)
        
        # Check what charts were generated
        print(f"\n📈 Generated charts: {list(charts.keys())}")
        
        # Test GDP trends chart
        if 'gdp_trends' in charts:
            gdp_chart = charts['gdp_trends']
            print(f"\n✅ GDP Trends Chart:")
            print(f"   - Title: {gdp_chart['title']}")
            print(f"   - Type: {gdp_chart['type']}")
            print(f"   - Data points: {len(gdp_chart['data']['x'])}")
            print(f"   - Period range: {gdp_chart['data']['x'][0]} to {gdp_chart['data']['x'][-1]}")
            print(f"   - Value range: {min(gdp_chart['data']['y']):.0f} to {max(gdp_chart['data']['y']):.0f} Million Dollars")
        else:
            print("❌ GDP Trends chart not found")
        
        # Test CPI trends chart
        if 'cpi_trends' in charts:
            cpi_chart = charts['cpi_trends']
            print(f"\n✅ CPI Trends Chart:")
            print(f"   - Title: {cpi_chart['title']}")
            print(f"   - Type: {cpi_chart['type']}")
            print(f"   - Data points: {len(cpi_chart['data']['x'])}")
            print(f"   - Period range: {cpi_chart['data']['x'][0]} to {cpi_chart['data']['x'][-1]}")
            print(f"   - Value range: {min(cpi_chart['data']['y']):.1f} to {max(cpi_chart['data']['y']):.1f}")
        else:
            print("❌ CPI Trends chart not found")
        
        # Test economic overview chart
        if 'economic_overview' in charts:
            overview_chart = charts['economic_overview']
            print(f"\n✅ Economic Overview Chart:")
            print(f"   - Title: {overview_chart['title']}")
            print(f"   - Type: {overview_chart['type']}")
            print(f"   - Series: {[series['name'] for series in overview_chart['data']]}")
            for series in overview_chart['data']:
                print(f"   - {series['name']}: {len(series['x'])} data points")
        else:
            print("❌ Economic Overview chart not found")
        
        # Check if old economic_trends chart still exists (should not)
        if 'economic_trends' in charts:
            print("\n⚠️  WARNING: Old 'economic_trends' chart still exists - this should be removed")
        else:
            print("\n✅ Old 'economic_trends' chart successfully removed")
        
        print("\n🎯 Summary:")
        print(f"   - Total charts generated: {len(charts)}")
        print(f"   - GDP chart: {'✅' if 'gdp_trends' in charts else '❌'}")
        print(f"   - CPI chart: {'✅' if 'cpi_trends' in charts else '❌'}")
        print(f"   - Overview chart: {'✅' if 'economic_overview' in charts else '❌'}")
        print(f"   - Old combined chart removed: {'✅' if 'economic_trends' not in charts else '❌'}")
        
        # Save test results
        test_results = {
            "timestamp": "2025-01-12T15:45:00",
            "test_status": "success",
            "charts_generated": list(charts.keys()),
            "gdp_chart_available": 'gdp_trends' in charts,
            "cpi_chart_available": 'cpi_trends' in charts,
            "overview_chart_available": 'economic_overview' in charts,
            "old_chart_removed": 'economic_trends' not in charts,
            "chart_details": {}
        }
        
        if 'gdp_trends' in charts:
            test_results["chart_details"]["gdp_trends"] = {
                "data_points": len(charts['gdp_trends']['data']['x']),
                "period_range": [charts['gdp_trends']['data']['x'][0], charts['gdp_trends']['data']['x'][-1]],
                "value_range": [min(charts['gdp_trends']['data']['y']), max(charts['gdp_trends']['data']['y'])]
            }
        
        if 'cpi_trends' in charts:
            test_results["chart_details"]["cpi_trends"] = {
                "data_points": len(charts['cpi_trends']['data']['x']),
                "period_range": [charts['cpi_trends']['data']['x'][0], charts['cpi_trends']['data']['x'][-1]],
                "value_range": [min(charts['cpi_trends']['data']['y']), max(charts['cpi_trends']['data']['y'])]
            }
        
        with open('/home/ngtianxun/bigData_project/analytics/dashboard_json_output/chart_structure_test_results.json', 'w') as f:
            json.dump(test_results, f, indent=2)
        
        print("\n💾 Test results saved to chart_structure_test_results.json")
        
    except Exception as e:
        print(f"❌ Error during testing: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_new_chart_structure()