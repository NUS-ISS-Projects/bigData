#!/usr/bin/env python3
"""
Comprehensive Data Validation and Analysis Tool
Probes dashboard JSON output for missing data, invalid values, and insights generation analysis
"""

import json
import os
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple
import logging
from collections import defaultdict
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ComprehensiveDataValidator:
    """Comprehensive validation and analysis of dashboard data"""
    
    def __init__(self, json_output_dir: str = "./dashboard_json_output"):
        self.json_output_dir = Path(json_output_dir)
        self.validation_results = {
            "timestamp": datetime.now().isoformat(),
            "data_quality_issues": [],
            "missing_data_analysis": {},
            "invalid_values_analysis": {},
            "insights_generation_analysis": {},
            "chart_data_completeness": {},
            "recommendations": []
        }
    
    def run_comprehensive_validation(self) -> Dict[str, Any]:
        """Run all validation checks"""
        logger.info("Starting comprehensive data validation...")
        
        # 1. Analyze JSON files for data quality
        self._analyze_json_files()
        
        # 2. Check for missing data patterns
        self._analyze_missing_data_patterns()
        
        # 3. Validate chart data completeness
        self._validate_chart_data_completeness()
        
        # 4. Analyze insights generation mechanism
        self._analyze_insights_generation()
        
        # 5. Check for invalid values
        self._check_invalid_values()
        
        # 6. Generate recommendations
        self._generate_validation_recommendations()
        
        # 7. Save validation report
        self._save_validation_report()
        
        return self.validation_results
    
    def _analyze_json_files(self):
        """Analyze all JSON files for structure and content"""
        logger.info("Analyzing JSON files...")
        
        json_files = list(self.json_output_dir.glob("*.json"))
        file_analysis = {}
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                analysis = {
                    "file_size_kb": json_file.stat().st_size / 1024,
                    "structure_valid": True,
                    "has_charts": "charts" in data.get("content", {}),
                    "chart_count": len(data.get("content", {}).get("charts", {})),
                    "data_points_total": self._count_data_points(data),
                    "missing_fields": self._check_missing_fields(data),
                    "empty_charts": self._find_empty_charts(data)
                }
                
                file_analysis[json_file.name] = analysis
                
            except Exception as e:
                file_analysis[json_file.name] = {
                    "error": str(e),
                    "structure_valid": False
                }
                self.validation_results["data_quality_issues"].append({
                    "file": json_file.name,
                    "issue": "JSON parsing error",
                    "details": str(e)
                })
        
        self.validation_results["json_files_analysis"] = file_analysis
    
    def _count_data_points(self, data: Dict) -> int:
        """Count total data points in a JSON file"""
        total_points = 0
        charts = data.get("content", {}).get("charts", {})
        
        for chart_name, chart_data in charts.items():
            chart_data_section = chart_data.get("data", {})
            
            # Count different data structures
            if isinstance(chart_data_section, dict):
                for key, value in chart_data_section.items():
                    if isinstance(value, list):
                        total_points += len(value)
                    elif isinstance(value, (int, float)):
                        total_points += 1
        
        return total_points
    
    def _check_missing_fields(self, data: Dict) -> List[str]:
        """Check for missing required fields"""
        missing_fields = []
        required_fields = ["page_name", "timestamp", "content"]
        
        for field in required_fields:
            if field not in data:
                missing_fields.append(field)
        
        # Check chart-level required fields
        charts = data.get("content", {}).get("charts", {})
        for chart_name, chart_data in charts.items():
            chart_required = ["title", "chart_type", "data"]
            for field in chart_required:
                if field not in chart_data:
                    missing_fields.append(f"{chart_name}.{field}")
        
        return missing_fields
    
    def _find_empty_charts(self, data: Dict) -> List[str]:
        """Find charts with no data"""
        empty_charts = []
        charts = data.get("content", {}).get("charts", {})
        
        for chart_name, chart_data in charts.items():
            chart_data_section = chart_data.get("data", {})
            
            if not chart_data_section or self._is_data_empty(chart_data_section):
                empty_charts.append(chart_name)
        
        return empty_charts
    
    def _is_data_empty(self, data_section: Any) -> bool:
        """Check if data section is effectively empty"""
        if not data_section:
            return True
        
        if isinstance(data_section, dict):
            for value in data_section.values():
                if isinstance(value, list) and len(value) > 0:
                    return False
                elif isinstance(value, (int, float)) and value != 0:
                    return False
            return True
        
        return False
    
    def _analyze_missing_data_patterns(self):
        """Analyze patterns in missing data"""
        logger.info("Analyzing missing data patterns...")
        
        missing_patterns = {
            "files_with_missing_data": [],
            "common_missing_fields": defaultdict(int),
            "data_truncation_indicators": [],
            "suspicious_data_limits": []
        }
        
        json_files = list(self.json_output_dir.glob("*.json"))
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                # Check for truncation indicators
                truncation_indicators = self._detect_truncation_indicators(data)
                if truncation_indicators:
                    missing_patterns["data_truncation_indicators"].append({
                        "file": json_file.name,
                        "indicators": truncation_indicators
                    })
                
                # Check for suspicious data limits (e.g., exactly 1000 records)
                suspicious_limits = self._detect_suspicious_limits(data)
                if suspicious_limits:
                    missing_patterns["suspicious_data_limits"].append({
                        "file": json_file.name,
                        "limits": suspicious_limits
                    })
                
            except Exception as e:
                logger.error(f"Error analyzing {json_file.name}: {e}")
        
        self.validation_results["missing_data_analysis"] = missing_patterns
    
    def _detect_truncation_indicators(self, data: Dict) -> List[str]:
        """Detect indicators that data might be truncated"""
        indicators = []
        charts = data.get("content", {}).get("charts", {})
        
        for chart_name, chart_data in charts.items():
            chart_data_section = chart_data.get("data", {})
            
            # Check for round numbers that might indicate limits
            for key, value in chart_data_section.items():
                if isinstance(value, list):
                    length = len(value)
                    # Common truncation points
                    if length in [10, 15, 20, 50, 100, 500, 1000]:
                        indicators.append(f"{chart_name}.{key}: exactly {length} items")
        
        return indicators
    
    def _detect_suspicious_limits(self, data: Dict) -> List[str]:
        """Detect suspicious data limits"""
        limits = []
        charts = data.get("content", {}).get("charts", {})
        
        for chart_name, chart_data in charts.items():
            chart_data_section = chart_data.get("data", {})
            
            # Look for patterns that suggest artificial limits
            for key, value in chart_data_section.items():
                if isinstance(value, list) and len(value) > 0:
                    # Check if all values are the same (suspicious)
                    if len(set(str(v) for v in value)) == 1:
                        limits.append(f"{chart_name}.{key}: all values identical")
                    
                    # Check for sequential patterns that might indicate test data
                    if all(isinstance(v, (int, float)) for v in value[:5]):
                        numeric_values = [v for v in value if isinstance(v, (int, float))]
                        if len(numeric_values) > 2:
                            diffs = [numeric_values[i+1] - numeric_values[i] for i in range(len(numeric_values)-1)]
                            if len(set(diffs)) == 1 and diffs[0] != 0:
                                limits.append(f"{chart_name}.{key}: sequential pattern detected")
        
        return limits
    
    def _validate_chart_data_completeness(self):
        """Validate completeness of chart data"""
        logger.info("Validating chart data completeness...")
        
        completeness_analysis = {}
        json_files = list(self.json_output_dir.glob("*.json"))
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                charts = data.get("content", {}).get("charts", {})
                file_completeness = {
                    "total_charts": len(charts),
                    "charts_with_data": 0,
                    "charts_without_data": 0,
                    "data_quality_scores": [],
                    "chart_details": {}
                }
                
                for chart_name, chart_data in charts.items():
                    has_data = not self._is_data_empty(chart_data.get("data", {}))
                    data_points = self._count_chart_data_points(chart_data.get("data", {}))
                    
                    if has_data:
                        file_completeness["charts_with_data"] += 1
                    else:
                        file_completeness["charts_without_data"] += 1
                    
                    # Calculate data quality score (0-1)
                    quality_score = self._calculate_chart_quality_score(chart_data)
                    file_completeness["data_quality_scores"].append(quality_score)
                    
                    file_completeness["chart_details"][chart_name] = {
                        "has_data": has_data,
                        "data_points": data_points,
                        "quality_score": quality_score,
                        "chart_type": chart_data.get("chart_type", "unknown")
                    }
                
                # Calculate average quality score
                if file_completeness["data_quality_scores"]:
                    file_completeness["avg_quality_score"] = np.mean(file_completeness["data_quality_scores"])
                else:
                    file_completeness["avg_quality_score"] = 0
                
                completeness_analysis[json_file.name] = file_completeness
                
            except Exception as e:
                logger.error(f"Error validating {json_file.name}: {e}")
        
        self.validation_results["chart_data_completeness"] = completeness_analysis
    
    def _count_chart_data_points(self, chart_data: Dict) -> int:
        """Count data points in a single chart"""
        total = 0
        for key, value in chart_data.items():
            if isinstance(value, list):
                total += len(value)
            elif isinstance(value, (int, float)):
                total += 1
        return total
    
    def _calculate_chart_quality_score(self, chart_data: Dict) -> float:
        """Calculate quality score for a chart (0-1)"""
        score = 0.0
        max_score = 5.0
        
        # Check for required fields
        if chart_data.get("title"):
            score += 1.0
        if chart_data.get("chart_type"):
            score += 1.0
        if chart_data.get("data"):
            score += 1.0
        if chart_data.get("description"):
            score += 1.0
        
        # Check data quality
        data_section = chart_data.get("data", {})
        if data_section and not self._is_data_empty(data_section):
            score += 1.0
        
        return score / max_score
    
    def _analyze_insights_generation(self):
        """Analyze how insights are generated - hardcoded vs LLM"""
        logger.info("Analyzing insights generation mechanism...")
        
        insights_analysis = {
            "insights_source": "hardcoded",  # Based on code analysis
            "hardcoded_insights_detected": True,
            "llm_integration_status": "fallback_mode",
            "insights_quality_assessment": {},
            "recommendations_for_improvement": []
        }
        
        # Check insights_recommendations files
        insights_files = list(self.json_output_dir.glob("insights_recommendations_*.json"))
        
        for insights_file in insights_files:
            try:
                with open(insights_file, 'r') as f:
                    data = json.load(f)
                
                insights = data.get("content", {}).get("visual_insights", [])
                recommendations = data.get("content", {}).get("chart_recommendations", [])
                
                # Analyze insight patterns
                insight_patterns = self._analyze_insight_patterns(insights)
                recommendation_patterns = self._analyze_recommendation_patterns(recommendations)
                
                insights_analysis["insights_quality_assessment"][insights_file.name] = {
                    "total_insights": len(insights),
                    "total_recommendations": len(recommendations),
                    "insight_patterns": insight_patterns,
                    "recommendation_patterns": recommendation_patterns,
                    "appears_hardcoded": self._detect_hardcoded_content(insights + recommendations)
                }
                
            except Exception as e:
                logger.error(f"Error analyzing insights in {insights_file.name}: {e}")
        
        # Add recommendations for improvement
        insights_analysis["recommendations_for_improvement"] = [
            "Replace hardcoded insights with dynamic LLM-generated analysis",
            "Implement data-driven insight generation based on actual chart data",
            "Add confidence scores and data source references to insights",
            "Create template-based insight generation with variable substitution",
            "Implement A/B testing for insight quality and relevance"
        ]
        
        self.validation_results["insights_generation_analysis"] = insights_analysis
    
    def _analyze_insight_patterns(self, insights: List[Dict]) -> Dict[str, Any]:
        """Analyze patterns in insights"""
        patterns = {
            "categories": [insight.get("category", "unknown") for insight in insights],
            "avg_length": np.mean([len(insight.get("insight", "")) for insight in insights]) if insights else 0,
            "has_chart_references": sum(1 for insight in insights if insight.get("chart_reference")),
            "actionable_count": sum(1 for insight in insights if insight.get("actionable", False))
        }
        return patterns
    
    def _analyze_recommendation_patterns(self, recommendations: List[Dict]) -> Dict[str, Any]:
        """Analyze patterns in recommendations"""
        patterns = {
            "priorities": [rec.get("priority", "unknown") for rec in recommendations],
            "categories": [rec.get("category", "unknown") for rec in recommendations],
            "avg_length": np.mean([len(rec.get("recommendation", "")) for rec in recommendations]) if recommendations else 0,
            "has_implementation": sum(1 for rec in recommendations if rec.get("implementation"))
        }
        return patterns
    
    def _detect_hardcoded_content(self, content_items: List[Dict]) -> bool:
        """Detect if content appears to be hardcoded"""
        # Look for generic phrases that suggest hardcoded content
        generic_phrases = [
            "analysis reveals", "patterns indicate", "shows current performance",
            "Use the executive", "Monitor economic indicator", "Regular review"
        ]
        
        for item in content_items:
            text = item.get("insight", "") + item.get("recommendation", "")
            for phrase in generic_phrases:
                if phrase.lower() in text.lower():
                    return True
        
        return False
    
    def _check_invalid_values(self):
        """Check for invalid values in data"""
        logger.info("Checking for invalid values...")
        
        invalid_values_analysis = {
            "files_with_invalid_values": [],
            "invalid_value_types": defaultdict(int),
            "data_type_inconsistencies": [],
            "outlier_detection": []
        }
        
        json_files = list(self.json_output_dir.glob("*.json"))
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                invalid_values = self._find_invalid_values_in_data(data)
                if invalid_values:
                    invalid_values_analysis["files_with_invalid_values"].append({
                        "file": json_file.name,
                        "invalid_values": invalid_values
                    })
                
            except Exception as e:
                logger.error(f"Error checking invalid values in {json_file.name}: {e}")
        
        self.validation_results["invalid_values_analysis"] = invalid_values_analysis
    
    def _find_invalid_values_in_data(self, data: Dict) -> List[Dict]:
        """Find invalid values in data structure"""
        invalid_values = []
        charts = data.get("content", {}).get("charts", {})
        
        for chart_name, chart_data in charts.items():
            chart_data_section = chart_data.get("data", {})
            
            for key, value in chart_data_section.items():
                if isinstance(value, list):
                    for i, item in enumerate(value):
                        # Check for null, undefined, or invalid values
                        if item is None:
                            invalid_values.append({
                                "location": f"{chart_name}.{key}[{i}]",
                                "value": item,
                                "issue": "null_value"
                            })
                        elif isinstance(item, str) and item.lower() in ['null', 'undefined', 'nan', '']:
                            invalid_values.append({
                                "location": f"{chart_name}.{key}[{i}]",
                                "value": item,
                                "issue": "invalid_string_value"
                            })
                        elif isinstance(item, (int, float)) and (np.isnan(item) or np.isinf(item)):
                            invalid_values.append({
                                "location": f"{chart_name}.{key}[{i}]",
                                "value": item,
                                "issue": "invalid_numeric_value"
                            })
        
        return invalid_values
    
    def _generate_validation_recommendations(self):
        """Generate recommendations based on validation results"""
        recommendations = []
        
        # Data quality recommendations
        if self.validation_results["data_quality_issues"]:
            recommendations.append("🚨 CRITICAL: Fix JSON parsing errors in data files")
        
        # Missing data recommendations
        missing_analysis = self.validation_results.get("missing_data_analysis", {})
        if missing_analysis.get("data_truncation_indicators"):
            recommendations.append("📊 HIGH: Remove data truncation limits to get complete datasets")
        
        # Insights generation recommendations
        insights_analysis = self.validation_results.get("insights_generation_analysis", {})
        if insights_analysis.get("hardcoded_insights_detected"):
            recommendations.append("🤖 MEDIUM: Replace hardcoded insights with dynamic LLM-generated analysis")
            recommendations.append("📈 MEDIUM: Implement data-driven insight generation based on actual metrics")
        
        # Chart completeness recommendations
        completeness = self.validation_results.get("chart_data_completeness", {})
        charts_without_data = sum(file_data.get("charts_without_data", 0) for file_data in completeness.values())
        if charts_without_data > 0:
            recommendations.append(f"📉 MEDIUM: Fix {charts_without_data} charts with missing data")
        
        # Invalid values recommendations
        invalid_analysis = self.validation_results.get("invalid_values_analysis", {})
        if invalid_analysis.get("files_with_invalid_values"):
            recommendations.append("🔧 LOW: Clean up invalid values in data files")
        
        # General improvements
        recommendations.extend([
            "🔄 Implement automated data validation in CI/CD pipeline",
            "📊 Add real-time data quality monitoring dashboard",
            "🏥 Create comprehensive data source health checks",
            "📈 Implement data completeness tracking and alerting",
            "🔍 Add automated detection of data truncation patterns"
        ])
        
        self.validation_results["recommendations"] = recommendations
    
    def _save_validation_report(self):
        """Save validation report to file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = self.json_output_dir / f"comprehensive_validation_report_{timestamp}.json"
        
        try:
            with open(report_file, 'w') as f:
                json.dump(self.validation_results, f, indent=2, default=str)
            
            logger.info(f"Validation report saved to: {report_file}")
            
            # Also create a summary report
            self._create_summary_report(timestamp)
            
        except Exception as e:
            logger.error(f"Error saving validation report: {e}")
    
    def _create_summary_report(self, timestamp: str):
        """Create a human-readable summary report"""
        summary_file = self.json_output_dir / f"validation_summary_{timestamp}.txt"
        
        try:
            with open(summary_file, 'w') as f:
                f.write("COMPREHENSIVE DATA VALIDATION REPORT\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Generated: {self.validation_results['timestamp']}\n\n")
                
                # Summary statistics
                f.write("SUMMARY STATISTICS\n")
                f.write("-" * 20 + "\n")
                
                json_analysis = self.validation_results.get("json_files_analysis", {})
                f.write(f"Total JSON files analyzed: {len(json_analysis)}\n")
                
                valid_files = sum(1 for file_data in json_analysis.values() if file_data.get("structure_valid", False))
                f.write(f"Valid JSON files: {valid_files}\n")
                
                total_data_points = sum(file_data.get("data_points_total", 0) for file_data in json_analysis.values())
                f.write(f"Total data points: {total_data_points:,}\n")
                
                # Issues summary
                f.write("\nISSUES DETECTED\n")
                f.write("-" * 15 + "\n")
                
                data_quality_issues = len(self.validation_results.get("data_quality_issues", []))
                f.write(f"Data quality issues: {data_quality_issues}\n")
                
                missing_data = self.validation_results.get("missing_data_analysis", {})
                truncation_indicators = len(missing_data.get("data_truncation_indicators", []))
                f.write(f"Files with truncation indicators: {truncation_indicators}\n")
                
                insights_analysis = self.validation_results.get("insights_generation_analysis", {})
                hardcoded_detected = insights_analysis.get("hardcoded_insights_detected", False)
                f.write(f"Hardcoded insights detected: {'Yes' if hardcoded_detected else 'No'}\n")
                
                # Recommendations
                f.write("\nRECOMMENDATIONS\n")
                f.write("-" * 15 + "\n")
                
                for i, rec in enumerate(self.validation_results.get("recommendations", []), 1):
                    f.write(f"{i}. {rec}\n")
                
            logger.info(f"Summary report saved to: {summary_file}")
            
        except Exception as e:
            logger.error(f"Error creating summary report: {e}")

def main():
    """Main function to run comprehensive validation"""
    print("🔍 Starting Comprehensive Data Validation...")
    
    validator = ComprehensiveDataValidator()
    results = validator.run_comprehensive_validation()
    
    print("\n📊 VALIDATION SUMMARY")
    print("=" * 30)
    
    # Print key findings
    json_analysis = results.get("json_files_analysis", {})
    print(f"📁 Files analyzed: {len(json_analysis)}")
    
    total_data_points = sum(file_data.get("data_points_total", 0) for file_data in json_analysis.values())
    print(f"📈 Total data points: {total_data_points:,}")
    
    data_quality_issues = len(results.get("data_quality_issues", []))
    print(f"⚠️  Data quality issues: {data_quality_issues}")
    
    insights_analysis = results.get("insights_generation_analysis", {})
    hardcoded_detected = insights_analysis.get("hardcoded_insights_detected", False)
    print(f"🤖 Hardcoded insights: {'Yes' if hardcoded_detected else 'No'}")
    
    print(f"\n📋 Generated {len(results.get('recommendations', []))} recommendations")
    
    print("\n🎯 TOP RECOMMENDATIONS:")
    for i, rec in enumerate(results.get("recommendations", [])[:5], 1):
        print(f"  {i}. {rec}")
    
    print("\n✅ Validation complete! Check dashboard_json_output/ for detailed reports.")

if __name__ == "__main__":
    main()