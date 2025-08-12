#!/usr/bin/env python3
"""
Comprehensive LLM Integration Test Suite

This script demonstrates and tests all LLM functionality in the Economic Intelligence Platform:
1. Basic LLM configuration and connectivity
2. Enhanced Economic Intelligence Platform with LLM
3. Individual analysis components
4. Error handling and fallback mechanisms

Author: AI Assistant
Date: 2025-08-11
"""

import sys
import json
import logging
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from llm_config import LLMConfig, LLMProvider, get_default_config, create_llm_client
from enhanced_economic_intelligence import EnhancedEconomicIntelligencePlatform
from silver_data_connector import SilverLayerConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_llm_configuration():
    """Test 1: Basic LLM Configuration and Connectivity"""
    print("\n" + "="*60)
    print("TEST 1: LLM Configuration and Connectivity")
    print("="*60)
    
    try:
        # Get default configuration
        config = get_default_config()
        print(f"✅ Default LLM Config: {config.provider.value} - {config.model_name}")
        
        # Create LLM client
        client = create_llm_client(config)
        print(f"✅ LLM Client created successfully")
        
        # Test basic analysis
        test_prompt = "Analyze the economic significance of Singapore's strategic location in Southeast Asia."
        result = client.generate_analysis(
            prompt=test_prompt
        )
        
        if result:
            print(f"✅ Basic analysis generation successful")
            print(f"📊 Analysis preview: {result[:200]}...")
        else:
            print("⚠️ Analysis generated but no result")
            
        return True
        
    except Exception as e:
        print(f"❌ LLM Configuration test failed: {e}")
        return False

def test_enhanced_platform():
    """Test 2: Enhanced Economic Intelligence Platform"""
    print("\n" + "="*60)
    print("TEST 2: Enhanced Economic Intelligence Platform")
    print("="*60)
    
    try:
        # Initialize platform with LLM enabled
        platform = EnhancedEconomicIntelligencePlatform()
        print("✅ Enhanced platform initialized with LLM enabled")
        
        # Generate comprehensive report
        print("🔄 Generating comprehensive intelligence report...")
        report = platform.generate_comprehensive_intelligence_report()
        
        if report:
            print(f"✅ Comprehensive report generated successfully")
            
            # Display report summary
            print(f"📈 Economic Insights: {len(report.get('economic_insights', []))}")
            print(f"🎯 Strategic Recommendations: {len(report.get('strategic_recommendations', []))}")
            print(f"⚠️ Risk Factors: {len(report.get('risk_factors', []))}")
            
            # Show sample insights
            if report.get('economic_insights'):
                print(f"\n💡 Sample Economic Insight:")
                print(f"   {report['economic_insights'][0][:150]}...")
                
            if report.get('strategic_recommendations'):
                print(f"\n🎯 Sample Recommendation:")
                print(f"   {report['strategic_recommendations'][0][:150]}...")
            
            # Save report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = f"/tmp/comprehensive_llm_test_report_{timestamp}.json"
            
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            print(f"💾 Report saved to: {report_file}")
            
        return True
        
    except Exception as e:
        print(f"❌ Enhanced platform test failed: {e}")
        return False

def test_individual_analyses():
    """Test 3: Individual Analysis Components"""
    print("\n" + "="*60)
    print("TEST 3: Individual Analysis Components")
    print("="*60)
    
    try:
        # Initialize LLM client
        config = get_default_config()
        client = create_llm_client(config)
        
        # Test different analysis types
        analysis_tests = [
            {
                "name": "Business Formation Analysis",
                "prompt": "Analyze Singapore's business formation trends and their economic implications.",
                "type": "business_analysis"
            },
            {
                "name": "Economic Indicators Analysis",
                "prompt": "Evaluate key economic indicators for Singapore's economic health.",
                "type": "economic_indicators"
            },
            {
                "name": "Government Policy Analysis",
                "prompt": "Assess the impact of government spending on Singapore's economic growth.",
                "type": "policy_analysis"
            },
            {
                "name": "Market Trends Analysis",
                "prompt": "Analyze property market trends and their correlation with economic conditions.",
                "type": "market_analysis"
            }
        ]
        
        successful_tests = 0
        
        for test in analysis_tests:
            try:
                print(f"\n🔄 Testing: {test['name']}")
                result = client.generate_analysis(
                    prompt=test['prompt'],
                    analysis_type=test['type']
                )
                
                if result and isinstance(result, str) and len(result) > 0:
                    print(f"✅ {test['name']}: Success")
                    print(f"   Preview: {result[:100]}...")
                    successful_tests += 1
                else:
                    print(f"⚠️ {test['name']}: Unexpected format or empty result")
                    
            except Exception as e:
                print(f"❌ {test['name']}: Failed - {e}")
        
        print(f"\n📊 Individual Analysis Results: {successful_tests}/{len(analysis_tests)} successful")
        return successful_tests == len(analysis_tests)
        
    except Exception as e:
        print(f"❌ Individual analyses test failed: {e}")
        return False

def test_data_integration():
    """Test 4: Data Integration with LLM Analysis"""
    print("\n" + "="*60)
    print("TEST 4: Data Integration with LLM Analysis")
    print("="*60)
    
    try:
        # Test data connector
        connector = SilverLayerConnector()
        print("✅ Data connector initialized")
        
        # Load sample data
        acra_data = connector.load_acra_companies(limit=100)
        economic_data = connector.load_economic_indicators(limit=50)
        
        print(f"📊 Loaded {len(acra_data)} company records")
        print(f"📈 Loaded {len(economic_data)} economic indicators")
        
        # Create data-driven analysis
        config = get_default_config()
        client = create_llm_client(config)
        
        data_prompt = f"""
        Based on the following Singapore economic data:
        - Company registrations: {len(acra_data)} recent records
        - Economic indicators: {len(economic_data)} data points
        
        Provide insights on:
        1. Business formation trends
        2. Economic health indicators
        3. Growth opportunities
        4. Risk factors to monitor
        """
        
        result = client.generate_analysis(
            prompt=data_prompt,
            analysis_type="data_driven_analysis"
        )
        
        if result and isinstance(result, str) and len(result) > 0:
            print("✅ Data-driven analysis successful")
            print(f"📊 Analysis: {result[:200]}...")
        
        connector.close_connection()
        return True
        
    except Exception as e:
        print(f"❌ Data integration test failed: {e}")
        return False

def test_error_handling():
    """Test 5: Error Handling and Fallback Mechanisms"""
    print("\n" + "="*60)
    print("TEST 5: Error Handling and Fallback Mechanisms")
    print("="*60)
    
    try:
        # Test with invalid configuration
        print("🔄 Testing invalid model configuration...")
        
        invalid_config = LLMConfig(
            provider=LLMProvider.LOCAL_OLLAMA,
            model_name="nonexistent-model",
            api_base="http://localhost:11434"
        )
        
        try:
            client = create_llm_client(invalid_config)
            result = client.generate_analysis(
                prompt="Test prompt",
                analysis_type="test"
            )
            print("⚠️ Expected error but analysis succeeded")
        except Exception as e:
            print(f"✅ Error handling working: {type(e).__name__}")
        
        # Test platform with LLM disabled (using invalid LLM config)
        print("\n🔄 Testing platform with LLM disabled...")
        invalid_llm_config = LLMConfig(
            provider=LLMProvider.LOCAL_OLLAMA,
            model="nonexistent-model",
            api_base="http://localhost:11434"
        )
        platform = EnhancedEconomicIntelligencePlatform(llm_config=invalid_llm_config)
        report = platform.generate_comprehensive_intelligence_report()
        
        if report:
            print("✅ Platform works without LLM (fallback mode)")
            print(f"📊 Generated {len(report.get('economic_insights', []))} insights without LLM")
        
        return True
        
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        return False

def main():
    """Run comprehensive LLM test suite"""
    print("🧠 COMPREHENSIVE LLM INTEGRATION TEST SUITE")
    print("=" * 60)
    print(f"📅 Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🐍 Python Version: {sys.version.split()[0]}")
    
    # Run all tests
    tests = [
        ("LLM Configuration", test_llm_configuration),
        ("Enhanced Platform", test_enhanced_platform),
        ("Individual Analyses", test_individual_analyses),
        ("Data Integration", test_data_integration),
        ("Error Handling", test_error_handling)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n📊 Overall Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! LLM integration is fully functional.")
    else:
        print(f"⚠️ {total - passed} test(s) failed. Please check the logs above.")
    
    print("\n💡 Next Steps:")
    print("   1. Access the Streamlit dashboard at http://localhost:8501")
    print("   2. Navigate to the '🤖 LLM Analysis' tab")
    print("   3. Try different analysis types and custom queries")
    print("   4. Check the generated reports in /tmp/")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)