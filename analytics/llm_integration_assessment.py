#!/usr/bin/env python3
"""
LLM Integration Assessment for Business Formation Dashboard
Validates dynamic LLM-generated insights and identifies remaining hardcoded content
"""

import json
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd

# Import LLM configuration
from llm_config import get_default_config, create_llm_client, EconomicAnalysisPrompts
from silver_data_connector import SilverLayerConnector, DataSourceConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LLMIntegrationAssessment:
    """Comprehensive assessment of LLM integration in the dashboard"""
    
    def __init__(self):
        self.json_output_dir = Path("./dashboard_json_output")
        self.assessment_results = {
            "timestamp": datetime.now().isoformat(),
            "llm_status": {},
            "business_formation_analysis": {},
            "hardcoded_content_detection": {},
            "dynamic_content_validation": {},
            "recommendations": []
        }
        
    def run_comprehensive_assessment(self) -> Dict[str, Any]:
        """Run complete LLM integration assessment"""
        logger.info("Starting LLM Integration Assessment...")
        
        # 1. Test LLM Configuration
        self._test_llm_configuration()
        
        # 2. Analyze Business Formation Content
        self._analyze_business_formation_content()
        
        # 3. Check for Hardcoded Content
        self._detect_hardcoded_content()
        
        # 4. Validate Dynamic Content Generation
        self._validate_dynamic_content()
        
        # 5. Generate Recommendations
        self._generate_recommendations()
        
        # 6. Save Assessment Report
        self._save_assessment_report()
        
        return self.assessment_results
    
    def _test_llm_configuration(self):
        """Test LLM configuration and availability"""
        logger.info("Testing LLM configuration...")
        
        try:
            config = get_default_config()
            client = create_llm_client(config)
            
            self.assessment_results["llm_status"] = {
                "provider": config.provider.value,
                "model_name": config.model_name,
                "api_base": config.api_base,
                "client_available": client.is_available(),
                "configuration_valid": True
            }
            
            # Test actual LLM generation
            if client.is_available():
                test_prompt = "Provide a brief analysis of Singapore's business formation trends."
                test_response = client.generate_analysis(test_prompt, {}, "test")
                
                self.assessment_results["llm_status"]["test_generation"] = {
                    "success": bool(test_response),
                    "response_length": len(test_response) if test_response else 0,
                    "sample_response": test_response[:200] if test_response else None
                }
            
        except Exception as e:
            logger.error(f"LLM configuration test failed: {e}")
            self.assessment_results["llm_status"] = {
                "configuration_valid": False,
                "error": str(e)
            }
    
    def _analyze_business_formation_content(self):
        """Analyze Business Formation content for LLM integration"""
        logger.info("Analyzing Business Formation content...")
        
        # Check for LLM insights JSON files
        llm_insights_files = list(self.json_output_dir.glob("business_formation_llm_insights_*.json"))
        
        analysis = {
            "llm_insights_files_found": len(llm_insights_files),
            "latest_llm_insights": None,
            "content_analysis": {}
        }
        
        if llm_insights_files:
            # Analyze the most recent LLM insights file
            latest_file = max(llm_insights_files, key=lambda x: x.stat().st_mtime)
            
            try:
                with open(latest_file, 'r') as f:
                    llm_data = json.load(f)
                
                analysis["latest_llm_insights"] = {
                    "file_path": str(latest_file),
                    "generation_timestamp": llm_data.get("content", {}).get("generation_timestamp"),
                    "llm_model": llm_data.get("content", {}).get("llm_model"),
                    "has_strategic_insights": bool(llm_data.get("content", {}).get("strategic_insights")),
                    "has_recommendations": bool(llm_data.get("content", {}).get("recommendations")),
                    "insights_length": len(llm_data.get("content", {}).get("strategic_insights", "")),
                    "recommendations_length": len(llm_data.get("content", {}).get("recommendations", ""))
                }
                
                # Analyze content quality
                insights_text = llm_data.get("content", {}).get("strategic_insights", "")
                recommendations_text = llm_data.get("content", {}).get("recommendations", "")
                
                analysis["content_analysis"] = {
                    "insights_contains_singapore": "singapore" in insights_text.lower(),
                    "insights_contains_business": "business" in insights_text.lower(),
                    "recommendations_actionable": any(word in recommendations_text.lower() for word in ["should", "recommend", "suggest", "implement"]),
                    "content_appears_dynamic": not any(phrase in insights_text.lower() for phrase in ["template", "placeholder", "example"])
                }
                
            except Exception as e:
                logger.error(f"Error analyzing LLM insights file: {e}")
                analysis["latest_llm_insights"] = {"error": str(e)}
        
        self.assessment_results["business_formation_analysis"] = analysis
    
    def _detect_hardcoded_content(self):
        """Detect remaining hardcoded content in JSON files"""
        logger.info("Detecting hardcoded content...")
        
        hardcoded_patterns = [
            "Geographic Concentration: Business formation shows clear regional preferences",
            "Entity Preferences: Local companies dominate the business landscape",
            "Seasonal Patterns: Registration activity varies throughout the year",
            "Underserved Regions: Potential for business development",
            "Entity Diversification: Opportunities for alternative business structures",
            "Timing Optimization: Strategic registration timing"
        ]
        
        detection_results = {
            "files_scanned": 0,
            "hardcoded_content_found": [],
            "dynamic_content_found": [],
            "total_hardcoded_instances": 0
        }
        
        # Scan all JSON files in output directory
        for json_file in self.json_output_dir.glob("*.json"):
            try:
                with open(json_file, 'r') as f:
                    content = f.read()
                
                detection_results["files_scanned"] += 1
                
                # Check for hardcoded patterns
                hardcoded_found = []
                for pattern in hardcoded_patterns:
                    if pattern in content:
                        hardcoded_found.append(pattern)
                        detection_results["total_hardcoded_instances"] += 1
                
                if hardcoded_found:
                    detection_results["hardcoded_content_found"].append({
                        "file": json_file.name,
                        "patterns": hardcoded_found
                    })
                
                # Check for dynamic content indicators
                dynamic_indicators = ["Generated by Llama", "AI-Generated", "llm_model", "generation_timestamp"]
                if any(indicator in content for indicator in dynamic_indicators):
                    detection_results["dynamic_content_found"].append(json_file.name)
                
            except Exception as e:
                logger.error(f"Error scanning file {json_file}: {e}")
        
        self.assessment_results["hardcoded_content_detection"] = detection_results
    
    def _validate_dynamic_content(self):
        """Validate that dynamic content is being generated properly"""
        logger.info("Validating dynamic content generation...")
        
        validation_results = {
            "llm_integration_active": False,
            "content_freshness": {},
            "content_quality_metrics": {}
        }
        
        # Check for recent LLM-generated content
        recent_threshold = datetime.now().timestamp() - 3600  # 1 hour ago
        
        llm_files = list(self.json_output_dir.glob("*llm_insights*.json"))
        
        if llm_files:
            recent_files = [f for f in llm_files if f.stat().st_mtime > recent_threshold]
            
            validation_results["content_freshness"] = {
                "total_llm_files": len(llm_files),
                "recent_llm_files": len(recent_files),
                "latest_file_age_minutes": (datetime.now().timestamp() - max(f.stat().st_mtime for f in llm_files)) / 60 if llm_files else None
            }
            
            if recent_files:
                validation_results["llm_integration_active"] = True
                
                # Analyze content quality of most recent file
                latest_file = max(llm_files, key=lambda x: x.stat().st_mtime)
                try:
                    with open(latest_file, 'r') as f:
                        data = json.load(f)
                    
                    content = data.get("content", {})
                    insights = content.get("strategic_insights", "")
                    recommendations = content.get("recommendations", "")
                    
                    validation_results["content_quality_metrics"] = {
                        "insights_word_count": len(insights.split()) if insights else 0,
                        "recommendations_word_count": len(recommendations.split()) if recommendations else 0,
                        "contains_specific_data": any(word in insights.lower() for word in ["singapore", "companies", "formation", "business"]),
                        "contains_actionable_recommendations": any(word in recommendations.lower() for word in ["should", "recommend", "implement", "consider"])
                    }
                    
                except Exception as e:
                    logger.error(f"Error validating content quality: {e}")
        
        self.assessment_results["dynamic_content_validation"] = validation_results
    
    def _generate_recommendations(self):
        """Generate recommendations based on assessment results"""
        recommendations = []
        
        # LLM Status Recommendations
        if not self.assessment_results["llm_status"].get("client_available", False):
            recommendations.append({
                "priority": "HIGH",
                "category": "LLM Configuration",
                "issue": "LLM client not available",
                "recommendation": "Check Ollama service status and ensure llama3.1:8b model is installed",
                "action": "Run 'ollama serve' and 'ollama pull llama3.1:8b'"
            })
        
        # Hardcoded Content Recommendations
        hardcoded_instances = self.assessment_results["hardcoded_content_detection"].get("total_hardcoded_instances", 0)
        if hardcoded_instances > 0:
            recommendations.append({
                "priority": "MEDIUM",
                "category": "Content Quality",
                "issue": f"Found {hardcoded_instances} hardcoded content instances",
                "recommendation": "Replace remaining hardcoded insights with dynamic LLM-generated content",
                "action": "Update dashboard code to use LLM for all insights generation"
            })
        
        # Dynamic Content Recommendations
        if not self.assessment_results["dynamic_content_validation"].get("llm_integration_active", False):
            recommendations.append({
                "priority": "HIGH",
                "category": "Dynamic Content",
                "issue": "LLM integration not actively generating content",
                "recommendation": "Ensure LLM client is properly initialized in dashboard",
                "action": "Check dashboard logs and LLM client initialization"
            })
        
        # Business Formation Specific Recommendations
        bf_analysis = self.assessment_results["business_formation_analysis"]
        if bf_analysis.get("llm_insights_files_found", 0) == 0:
            recommendations.append({
                "priority": "HIGH",
                "category": "Business Formation",
                "issue": "No LLM insights files found for Business Formation",
                "recommendation": "Verify Business Formation tab is generating LLM insights",
                "action": "Navigate to Business Formation tab and check for AI-generated insights"
            })
        
        # Success Recommendations
        if (self.assessment_results["llm_status"].get("client_available", False) and 
            self.assessment_results["dynamic_content_validation"].get("llm_integration_active", False)):
            recommendations.append({
                "priority": "LOW",
                "category": "Enhancement",
                "issue": "LLM integration working well",
                "recommendation": "Consider expanding LLM integration to other dashboard sections",
                "action": "Apply similar LLM integration to Economic Indicators, Government Spending, and Property Market tabs"
            })
        
        self.assessment_results["recommendations"] = recommendations
    
    def _save_assessment_report(self):
        """Save assessment report to JSON file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = self.json_output_dir / f"llm_integration_assessment_{timestamp}.json"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(self.assessment_results, f, indent=2, default=str)
            
            logger.info(f"Assessment report saved: {report_file}")
            
        except Exception as e:
            logger.error(f"Error saving assessment report: {e}")
    
    def print_summary(self):
        """Print assessment summary to console"""
        print("\n" + "="*60)
        print("🤖 LLM INTEGRATION ASSESSMENT SUMMARY")
        print("="*60)
        
        # LLM Status
        llm_status = self.assessment_results["llm_status"]
        print(f"\n📊 LLM Configuration:")
        print(f"   Provider: {llm_status.get('provider', 'Unknown')}")
        print(f"   Model: {llm_status.get('model_name', 'Unknown')}")
        print(f"   Available: {'✅ Yes' if llm_status.get('client_available') else '❌ No'}")
        
        # Business Formation Analysis
        bf_analysis = self.assessment_results["business_formation_analysis"]
        print(f"\n🏢 Business Formation Analysis:")
        print(f"   LLM Insights Files: {bf_analysis.get('llm_insights_files_found', 0)}")
        
        if bf_analysis.get("latest_llm_insights"):
            latest = bf_analysis["latest_llm_insights"]
            print(f"   Latest Generation: {latest.get('generation_timestamp', 'Unknown')}")
            print(f"   Model Used: {latest.get('llm_model', 'Unknown')}")
        
        # Hardcoded Content Detection
        hardcoded = self.assessment_results["hardcoded_content_detection"]
        print(f"\n🔍 Hardcoded Content Detection:")
        print(f"   Files Scanned: {hardcoded.get('files_scanned', 0)}")
        print(f"   Hardcoded Instances: {hardcoded.get('total_hardcoded_instances', 0)}")
        print(f"   Dynamic Content Files: {len(hardcoded.get('dynamic_content_found', []))}")
        
        # Dynamic Content Validation
        dynamic = self.assessment_results["dynamic_content_validation"]
        print(f"\n⚡ Dynamic Content Validation:")
        print(f"   LLM Integration Active: {'✅ Yes' if dynamic.get('llm_integration_active') else '❌ No'}")
        
        # Recommendations
        recommendations = self.assessment_results["recommendations"]
        print(f"\n📋 Recommendations ({len(recommendations)} total):")
        
        for rec in recommendations:
            priority_icon = "🔴" if rec["priority"] == "HIGH" else "🟡" if rec["priority"] == "MEDIUM" else "🟢"
            print(f"   {priority_icon} {rec['category']}: {rec['issue']}")
            print(f"      → {rec['recommendation']}")
        
        print("\n" + "="*60)

def main():
    """Main function to run LLM integration assessment"""
    assessment = LLMIntegrationAssessment()
    results = assessment.run_comprehensive_assessment()
    assessment.print_summary()
    
    return results

if __name__ == "__main__":
    main()