#!/usr/bin/env python3
"""
Comprehensive JSON Data Validator for Economic Intelligence Dashboard
Analyzes all JSON outputs for missing data, invalid values, and quality issues
"""

import json
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ComprehensiveJSONValidator:
    """Comprehensive validator for dashboard JSON outputs"""
    
    def __init__(self, json_output_dir: str = "./dashboard_json_output"):
        self.json_output_dir = Path(json_output_dir)
        self.validation_results = {
            "timestamp": datetime.now().isoformat(),
            "files_analyzed": 0,
            "validation_summary": {},
            "business_formation_analysis": {},
            "data_quality_issues": [],
            "missing_data_points": [],
            "invalid_values": [],
            "recommendations": []
        }
        
    def run_comprehensive_validation(self) -> Dict[str, Any]:
        """Run complete JSON validation analysis"""
        logger.info("Starting Comprehensive JSON Validation...")
        
        # 1. Analyze all JSON files
        self._analyze_all_json_files()
        
        # 2. Deep dive into Business Formation data
        self._analyze_business_formation_data()
        
        # 3. Validate LLM insights quality
        self._validate_llm_insights_quality()
        
        # 4. Check for data consistency
        self._check_data_consistency()
        
        # 5. Generate recommendations
        self._generate_validation_recommendations()
        
        # 6. Save validation report
        self._save_validation_report()
        
        return self.validation_results
    
    def _analyze_all_json_files(self):
        """Analyze all JSON files for basic structure and content"""
        logger.info("Analyzing all JSON files...")
        
        json_files = list(self.json_output_dir.glob("*.json"))
        self.validation_results["files_analyzed"] = len(json_files)
        
        file_analysis = {}
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                analysis = self._analyze_single_file(json_file.name, data)
                file_analysis[json_file.name] = analysis
                
            except Exception as e:
                logger.error(f"Error analyzing {json_file}: {e}")
                file_analysis[json_file.name] = {"error": str(e)}
        
        self.validation_results["validation_summary"] = file_analysis
    
    def _analyze_single_file(self, filename: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze a single JSON file for quality issues"""
        analysis = {
            "file_size_kb": round(len(json.dumps(data)) / 1024, 2),
            "has_content": bool(data.get("content")),
            "has_metadata": bool(data.get("metadata")),
            "timestamp_valid": bool(data.get("timestamp")),
            "content_sections": [],
            "data_quality_score": 0,
            "issues": []
        }
        
        # Analyze content structure
        content = data.get("content", {})
        if isinstance(content, dict):
            analysis["content_sections"] = list(content.keys())
            
            # Check for charts data
            if "charts" in content:
                charts = content["charts"]
                analysis["total_charts"] = len(charts) if isinstance(charts, dict) else 0
                analysis["charts_with_data"] = 0
                analysis["empty_charts"] = []
                
                if isinstance(charts, dict):
                    for chart_name, chart_data in charts.items():
                        if self._has_valid_chart_data(chart_data):
                            analysis["charts_with_data"] += 1
                        else:
                            analysis["empty_charts"].append(chart_name)
                            analysis["issues"].append(f"Chart '{chart_name}' has no valid data")
            
            # Check for LLM insights
            if "strategic_insights" in content or "recommendations" in content:
                analysis["has_llm_insights"] = True
                analysis["insights_length"] = len(content.get("strategic_insights", ""))
                analysis["recommendations_length"] = len(content.get("recommendations", ""))
                
                if analysis["insights_length"] < 100:
                    analysis["issues"].append("Strategic insights too short")
                if analysis["recommendations_length"] < 100:
                    analysis["issues"].append("Recommendations too short")
            else:
                analysis["has_llm_insights"] = False
        
        # Calculate data quality score
        score = 100
        score -= len(analysis["issues"]) * 10
        score -= len(analysis.get("empty_charts", [])) * 5
        if not analysis["has_content"]:
            score -= 30
        if not analysis["has_metadata"]:
            score -= 10
        
        analysis["data_quality_score"] = max(0, score)
        
        return analysis
    
    def _has_valid_chart_data(self, chart_data: Dict[str, Any]) -> bool:
        """Check if chart has valid data"""
        if not isinstance(chart_data, dict):
            return False
        
        data = chart_data.get("data", {})
        if not isinstance(data, dict):
            return False
        
        # Check for common data patterns
        has_values = bool(data.get("values")) or bool(data.get("y")) or bool(data.get("z"))
        has_labels = bool(data.get("labels")) or bool(data.get("x"))
        
        return has_values or has_labels
    
    def _analyze_business_formation_data(self):
        """Deep analysis of Business Formation specific data"""
        logger.info("Analyzing Business Formation data...")
        
        bf_files = list(self.json_output_dir.glob("business_formation*.json"))
        
        analysis = {
            "total_bf_files": len(bf_files),
            "llm_insights_files": 0,
            "regular_data_files": 0,
            "data_completeness": {},
            "chart_analysis": {},
            "insights_quality": {}
        }
        
        for bf_file in bf_files:
            try:
                with open(bf_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if "llm_insights" in bf_file.name:
                    analysis["llm_insights_files"] += 1
                    self._analyze_llm_insights_file(data, analysis)
                else:
                    analysis["regular_data_files"] += 1
                    self._analyze_bf_data_file(data, analysis)
                    
            except Exception as e:
                logger.error(f"Error analyzing Business Formation file {bf_file}: {e}")
        
        self.validation_results["business_formation_analysis"] = analysis
    
    def _analyze_llm_insights_file(self, data: Dict[str, Any], analysis: Dict[str, Any]):
        """Analyze LLM insights file quality"""
        content = data.get("content", {})
        
        insights_quality = {
            "has_strategic_insights": bool(content.get("strategic_insights")),
            "has_recommendations": bool(content.get("recommendations")),
            "generation_timestamp": content.get("generation_timestamp"),
            "llm_model": content.get("llm_model"),
            "context_data_available": bool(content.get("context_data"))
        }
        
        # Analyze content quality
        insights_text = content.get("strategic_insights", "")
        recommendations_text = content.get("recommendations", "")
        
        insights_quality.update({
            "insights_word_count": len(insights_text.split()),
            "recommendations_word_count": len(recommendations_text.split()),
            "contains_singapore_context": "singapore" in insights_text.lower(),
            "contains_business_terms": any(term in insights_text.lower() for term in ["business", "company", "formation", "registration"]),
            "has_actionable_recommendations": any(word in recommendations_text.lower() for word in ["should", "recommend", "implement", "consider", "establish"])
        })
        
        analysis["insights_quality"] = insights_quality
    
    def _analyze_bf_data_file(self, data: Dict[str, Any], analysis: Dict[str, Any]):
        """Analyze Business Formation data file"""
        content = data.get("content", {})
        charts = content.get("charts", {})
        
        chart_analysis = {
            "total_charts": len(charts),
            "charts_with_data": 0,
            "empty_charts": [],
            "chart_types": [],
            "data_points_summary": {}
        }
        
        for chart_name, chart_data in charts.items():
            if self._has_valid_chart_data(chart_data):
                chart_analysis["charts_with_data"] += 1
                chart_type = chart_data.get("chart_type", "unknown")
                chart_analysis["chart_types"].append(chart_type)
                
                # Analyze data points
                data_section = chart_data.get("data", {})
                if "values" in data_section:
                    values = data_section["values"]
                    if isinstance(values, list) and values:
                        chart_analysis["data_points_summary"][chart_name] = {
                            "count": len(values),
                            "min_value": min(values) if all(isinstance(v, (int, float)) for v in values) else None,
                            "max_value": max(values) if all(isinstance(v, (int, float)) for v in values) else None
                        }
            else:
                chart_analysis["empty_charts"].append(chart_name)
        
        analysis["chart_analysis"] = chart_analysis
    
    def _validate_llm_insights_quality(self):
        """Validate quality of LLM-generated insights"""
        logger.info("Validating LLM insights quality...")
        
        llm_files = list(self.json_output_dir.glob("*llm_insights*.json"))
        
        for llm_file in llm_files:
            try:
                with open(llm_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                content = data.get("content", {})
                insights = content.get("strategic_insights", "")
                recommendations = content.get("recommendations", "")
                
                # Check for quality issues
                issues = []
                
                if len(insights) < 500:
                    issues.append("Strategic insights too brief")
                
                if len(recommendations) < 300:
                    issues.append("Recommendations too brief")
                
                if "singapore" not in insights.lower():
                    issues.append("Missing Singapore context in insights")
                
                if not any(word in recommendations.lower() for word in ["recommend", "should", "implement"]):
                    issues.append("Recommendations lack actionable language")
                
                if issues:
                    self.validation_results["data_quality_issues"].extend([
                        {"file": llm_file.name, "issue": issue} for issue in issues
                    ])
                    
            except Exception as e:
                logger.error(f"Error validating LLM insights {llm_file}: {e}")
    
    def _check_data_consistency(self):
        """Check for data consistency across files"""
        logger.info("Checking data consistency...")
        
        # Check timestamp consistency
        timestamps = []
        for json_file in self.json_output_dir.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                timestamp = data.get("timestamp")
                if timestamp:
                    timestamps.append((json_file.name, timestamp))
            except Exception:
                continue
        
        # Group by timestamp to check consistency
        timestamp_groups = {}
        for filename, timestamp in timestamps:
            if timestamp not in timestamp_groups:
                timestamp_groups[timestamp] = []
            timestamp_groups[timestamp].append(filename)
        
        # Check for orphaned files
        for timestamp, files in timestamp_groups.items():
            if len(files) == 1 and "llm_insights" not in files[0]:
                self.validation_results["data_quality_issues"].append({
                    "issue": "Orphaned file with unique timestamp",
                    "file": files[0],
                    "timestamp": timestamp
                })
    
    def _generate_validation_recommendations(self):
        """Generate recommendations based on validation results"""
        recommendations = []
        
        # Check overall data quality
        validation_summary = self.validation_results["validation_summary"]
        low_quality_files = [filename for filename, analysis in validation_summary.items() 
                           if analysis.get("data_quality_score", 0) < 70]
        
        if low_quality_files:
            recommendations.append({
                "priority": "HIGH",
                "category": "Data Quality",
                "issue": f"{len(low_quality_files)} files have low quality scores",
                "recommendation": "Review and improve data generation for low-quality files",
                "affected_files": low_quality_files
            })
        
        # Check Business Formation specific issues
        bf_analysis = self.validation_results["business_formation_analysis"]
        if bf_analysis.get("llm_insights_files", 0) == 0:
            recommendations.append({
                "priority": "HIGH",
                "category": "Business Formation",
                "issue": "No LLM insights files found",
                "recommendation": "Ensure LLM integration is working for Business Formation tab"
            })
        
        # Check for empty charts
        empty_chart_files = []
        for filename, analysis in validation_summary.items():
            if analysis.get("empty_charts"):
                empty_chart_files.append(filename)
        
        if empty_chart_files:
            recommendations.append({
                "priority": "MEDIUM",
                "category": "Chart Data",
                "issue": f"{len(empty_chart_files)} files have empty charts",
                "recommendation": "Investigate data source issues for empty charts",
                "affected_files": empty_chart_files
            })
        
        self.validation_results["recommendations"] = recommendations
    
    def _save_validation_report(self):
        """Save validation report to JSON file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = self.json_output_dir / f"comprehensive_validation_report_{timestamp}.json"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(self.validation_results, f, indent=2, default=str)
            
            logger.info(f"Validation report saved: {report_file}")
            
        except Exception as e:
            logger.error(f"Error saving validation report: {e}")
    
    def print_validation_summary(self):
        """Print validation summary to console"""
        print("\n" + "="*70)
        print("📊 COMPREHENSIVE JSON VALIDATION SUMMARY")
        print("="*70)
        
        # Overall statistics
        print(f"\n📁 Files Analyzed: {self.validation_results['files_analyzed']}")
        
        # Data quality overview
        validation_summary = self.validation_results["validation_summary"]
        quality_scores = [analysis.get("data_quality_score", 0) for analysis in validation_summary.values() if isinstance(analysis, dict)]
        
        if quality_scores:
            avg_quality = sum(quality_scores) / len(quality_scores)
            print(f"📈 Average Data Quality Score: {avg_quality:.1f}/100")
        
        # Business Formation analysis
        bf_analysis = self.validation_results["business_formation_analysis"]
        print(f"\n🏢 Business Formation Analysis:")
        print(f"   Total BF Files: {bf_analysis.get('total_bf_files', 0)}")
        print(f"   LLM Insights Files: {bf_analysis.get('llm_insights_files', 0)}")
        print(f"   Regular Data Files: {bf_analysis.get('regular_data_files', 0)}")
        
        # Issues summary
        issues = self.validation_results["data_quality_issues"]
        print(f"\n⚠️  Data Quality Issues: {len(issues)}")
        for issue in issues[:5]:  # Show first 5 issues
            print(f"   • {issue.get('issue', 'Unknown issue')}")
        
        if len(issues) > 5:
            print(f"   ... and {len(issues) - 5} more issues")
        
        # Recommendations
        recommendations = self.validation_results["recommendations"]
        print(f"\n📋 Recommendations ({len(recommendations)} total):")
        
        for rec in recommendations:
            priority_icon = "🔴" if rec["priority"] == "HIGH" else "🟡" if rec["priority"] == "MEDIUM" else "🟢"
            print(f"   {priority_icon} {rec['category']}: {rec['issue']}")
            print(f"      → {rec['recommendation']}")
        
        print("\n" + "="*70)

def main():
    """Main function to run comprehensive JSON validation"""
    validator = ComprehensiveJSONValidator()
    results = validator.run_comprehensive_validation()
    validator.print_validation_summary()
    
    return results

if __name__ == "__main__":
    main()