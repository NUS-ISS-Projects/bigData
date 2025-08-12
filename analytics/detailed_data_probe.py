#!/usr/bin/env python3
"""
Detailed Data Probing Script
Focuses on specific data quality issues and provides deep analysis
"""

import json
import os
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DetailedDataProbe:
    """Detailed probing of specific data issues"""
    
    def __init__(self, json_output_dir: str = "./dashboard_json_output"):
        self.json_output_dir = Path(json_output_dir)
        self.probe_results = {
            "timestamp": datetime.now().isoformat(),
            "economic_indicators_structure_analysis": {},
            "data_truncation_detailed_analysis": {},
            "hardcoded_insights_detailed_analysis": {},
            "missing_data_patterns": {},
            "data_consistency_analysis": {}
        }
    
    def run_detailed_probe(self):
        """Run detailed probing analysis"""
        logger.info("Starting detailed data probe...")
        
        # 1. Analyze economic indicators structure issues
        self._analyze_economic_indicators_structure()
        
        # 2. Deep dive into data truncation patterns
        self._analyze_data_truncation_patterns()
        
        # 3. Detailed analysis of hardcoded insights
        self._analyze_hardcoded_insights_detailed()
        
        # 4. Check data consistency across time periods
        self._analyze_data_consistency()
        
        # 5. Identify missing data patterns
        self._identify_missing_data_patterns()
        
        # 6. Save detailed probe results
        self._save_probe_results()
        
        return self.probe_results
    
    def _analyze_economic_indicators_structure(self):
        """Analyze the structure issues in economic indicators files"""
        logger.info("Analyzing economic indicators structure...")
        
        economic_files = list(self.json_output_dir.glob("economic_indicators_*.json"))
        structure_analysis = {
            "total_files": len(economic_files),
            "structure_issues": [],
            "data_format_analysis": {},
            "common_patterns": []
        }
        
        for file_path in economic_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                # Analyze the structure
                file_analysis = {
                    "file_name": file_path.name,
                    "top_level_keys": list(data.keys()),
                    "content_structure": {},
                    "data_types": {},
                    "issues_found": []
                }
                
                # Check content structure
                if "content" in data:
                    content = data["content"]
                    file_analysis["content_structure"] = {
                        "type": type(content).__name__,
                        "keys_if_dict": list(content.keys()) if isinstance(content, dict) else None,
                        "length_if_list": len(content) if isinstance(content, list) else None,
                        "sample_items": content[:3] if isinstance(content, list) else None
                    }
                    
                    # Check for charts structure
                    if isinstance(content, dict) and "charts" in content:
                        charts = content["charts"]
                        file_analysis["charts_analysis"] = {
                            "charts_type": type(charts).__name__,
                            "charts_structure": self._analyze_charts_structure(charts)
                        }
                    elif isinstance(content, list):
                        file_analysis["issues_found"].append("Content is a list instead of expected dict with charts")
                
                structure_analysis["data_format_analysis"][file_path.name] = file_analysis
                
            except Exception as e:
                structure_analysis["structure_issues"].append({
                    "file": file_path.name,
                    "error": str(e)
                })
        
        self.probe_results["economic_indicators_structure_analysis"] = structure_analysis
    
    def _analyze_charts_structure(self, charts_data: Any) -> Dict[str, Any]:
        """Analyze the structure of charts data"""
        analysis = {
            "type": type(charts_data).__name__,
            "is_valid_structure": False,
            "chart_names": [],
            "structure_details": {}
        }
        
        if isinstance(charts_data, dict):
            analysis["is_valid_structure"] = True
            analysis["chart_names"] = list(charts_data.keys())
            
            for chart_name, chart_data in charts_data.items():
                analysis["structure_details"][chart_name] = {
                    "has_title": "title" in chart_data,
                    "has_chart_type": "chart_type" in chart_data,
                    "has_data": "data" in chart_data,
                    "data_structure": type(chart_data.get("data", {})).__name__
                }
        elif isinstance(charts_data, list):
            analysis["structure_details"] = {
                "list_length": len(charts_data),
                "sample_items": charts_data[:3] if charts_data else []
            }
        
        return analysis
    
    def _analyze_data_truncation_patterns(self):
        """Deep analysis of data truncation patterns"""
        logger.info("Analyzing data truncation patterns...")
        
        truncation_analysis = {
            "truncation_limits_detected": {},
            "potential_data_loss": [],
            "truncation_impact_assessment": {},
            "recommendations": []
        }
        
        # Common truncation limits to check
        common_limits = [10, 15, 20, 25, 50, 100, 500, 1000]
        
        json_files = list(self.json_output_dir.glob("*.json"))
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                file_truncation = self._detect_file_truncation(data, common_limits)
                if file_truncation:
                    truncation_analysis["truncation_limits_detected"][json_file.name] = file_truncation
                    
                    # Assess potential data loss
                    data_loss_assessment = self._assess_data_loss_impact(file_truncation)
                    if data_loss_assessment:
                        truncation_analysis["potential_data_loss"].append({
                            "file": json_file.name,
                            "assessment": data_loss_assessment
                        })
                
            except Exception as e:
                logger.error(f"Error analyzing truncation in {json_file.name}: {e}")
        
        # Generate recommendations
        if truncation_analysis["truncation_limits_detected"]:
            truncation_analysis["recommendations"] = [
                "Remove hardcoded LIMIT clauses in SQL queries",
                "Implement pagination instead of truncation",
                "Add configuration for maximum data points per chart",
                "Implement data sampling strategies for large datasets",
                "Add warnings when data is truncated"
            ]
        
        self.probe_results["data_truncation_detailed_analysis"] = truncation_analysis
    
    def _detect_file_truncation(self, data: Dict, common_limits: List[int]) -> Dict[str, Any]:
        """Detect truncation patterns in a file"""
        truncation_info = {
            "suspected_truncations": [],
            "exact_limit_matches": [],
            "data_distribution": {}
        }
        
        charts = data.get("content", {}).get("charts", {})
        
        for chart_name, chart_data in charts.items():
            chart_data_section = chart_data.get("data", {})
            
            for key, value in chart_data_section.items():
                if isinstance(value, list):
                    length = len(value)
                    
                    # Check for exact matches with common limits
                    if length in common_limits:
                        truncation_info["exact_limit_matches"].append({
                            "location": f"{chart_name}.{key}",
                            "length": length,
                            "suspected_limit": length
                        })
                    
                    # Check for suspicious patterns
                    if length > 5:  # Only check arrays with reasonable size
                        pattern_analysis = self._analyze_data_pattern(value)
                        if pattern_analysis["suspicious"]:
                            truncation_info["suspected_truncations"].append({
                                "location": f"{chart_name}.{key}",
                                "length": length,
                                "pattern": pattern_analysis
                            })
        
        return truncation_info if (truncation_info["suspected_truncations"] or truncation_info["exact_limit_matches"]) else None
    
    def _analyze_data_pattern(self, data_array: List) -> Dict[str, Any]:
        """Analyze data patterns to detect artificial limits"""
        pattern_analysis = {
            "suspicious": False,
            "reasons": [],
            "data_characteristics": {}
        }
        
        if not data_array:
            return pattern_analysis
        
        # Check for identical values
        unique_values = len(set(str(v) for v in data_array))
        if unique_values == 1:
            pattern_analysis["suspicious"] = True
            pattern_analysis["reasons"].append("All values are identical")
        
        # Check for sequential patterns in numeric data
        numeric_values = [v for v in data_array if isinstance(v, (int, float))]
        if len(numeric_values) > 3:
            diffs = [numeric_values[i+1] - numeric_values[i] for i in range(len(numeric_values)-1)]
            if len(set(diffs)) == 1 and diffs[0] != 0:
                pattern_analysis["suspicious"] = True
                pattern_analysis["reasons"].append(f"Sequential pattern with constant difference: {diffs[0]}")
        
        pattern_analysis["data_characteristics"] = {
            "total_items": len(data_array),
            "unique_values": unique_values,
            "data_types": list(set(type(v).__name__ for v in data_array))
        }
        
        return pattern_analysis
    
    def _assess_data_loss_impact(self, truncation_info: Dict) -> Dict[str, Any]:
        """Assess the impact of data truncation"""
        impact_assessment = {
            "severity": "low",
            "affected_charts": len(truncation_info.get("exact_limit_matches", [])),
            "potential_missing_data": [],
            "business_impact": []
        }
        
        # Determine severity based on number of affected charts
        if impact_assessment["affected_charts"] > 5:
            impact_assessment["severity"] = "high"
        elif impact_assessment["affected_charts"] > 2:
            impact_assessment["severity"] = "medium"
        
        # Assess business impact
        for match in truncation_info.get("exact_limit_matches", []):
            if "geographic" in match["location"].lower() or "region" in match["location"].lower():
                impact_assessment["business_impact"].append("Geographic analysis may be incomplete")
            elif "company" in match["location"].lower() or "business" in match["location"].lower():
                impact_assessment["business_impact"].append("Business formation analysis may be incomplete")
        
        return impact_assessment
    
    def _analyze_hardcoded_insights_detailed(self):
        """Detailed analysis of hardcoded insights"""
        logger.info("Analyzing hardcoded insights in detail...")
        
        insights_analysis = {
            "hardcoded_content_examples": [],
            "insight_quality_metrics": {},
            "recommendation_analysis": {},
            "improvement_suggestions": []
        }
        
        insights_files = list(self.json_output_dir.glob("insights_recommendations_*.json"))
        
        for insights_file in insights_files:
            try:
                with open(insights_file, 'r') as f:
                    data = json.load(f)
                
                content = data.get("content", {})
                insights = content.get("visual_insights", [])
                recommendations = content.get("chart_recommendations", [])
                
                # Analyze insights
                for insight in insights:
                    insight_text = insight.get("insight", "")
                    if self._is_hardcoded_text(insight_text):
                        insights_analysis["hardcoded_content_examples"].append({
                            "file": insights_file.name,
                            "category": insight.get("category", "unknown"),
                            "text": insight_text[:100] + "..." if len(insight_text) > 100 else insight_text,
                            "hardcoded_indicators": self._identify_hardcoded_indicators(insight_text)
                        })
                
                # Analyze recommendations
                for rec in recommendations:
                    rec_text = rec.get("recommendation", "")
                    if self._is_hardcoded_text(rec_text):
                        insights_analysis["hardcoded_content_examples"].append({
                            "file": insights_file.name,
                            "category": rec.get("category", "unknown"),
                            "text": rec_text[:100] + "..." if len(rec_text) > 100 else rec_text,
                            "hardcoded_indicators": self._identify_hardcoded_indicators(rec_text)
                        })
                
            except Exception as e:
                logger.error(f"Error analyzing insights in {insights_file.name}: {e}")
        
        # Generate improvement suggestions
        insights_analysis["improvement_suggestions"] = [
            "Replace static text with dynamic templates that use actual data values",
            "Implement LLM-based insight generation using chart data as context",
            "Create insight templates with variable placeholders for data-driven content",
            "Add data validation to ensure insights match actual chart data",
            "Implement insight versioning and A/B testing for quality improvement"
        ]
        
        self.probe_results["hardcoded_insights_detailed_analysis"] = insights_analysis
    
    def _is_hardcoded_text(self, text: str) -> bool:
        """Check if text appears to be hardcoded"""
        hardcoded_indicators = [
            "analysis reveals", "patterns indicate", "shows current performance",
            "use the executive", "monitor economic indicator", "regular review",
            "dashboard provides", "comprehensive view", "strategic insights"
        ]
        
        text_lower = text.lower()
        return any(indicator in text_lower for indicator in hardcoded_indicators)
    
    def _identify_hardcoded_indicators(self, text: str) -> List[str]:
        """Identify specific indicators that suggest hardcoded content"""
        indicators = []
        text_lower = text.lower()
        
        hardcoded_phrases = {
            "generic_language": ["analysis reveals", "patterns indicate", "shows current"],
            "template_language": ["use the", "monitor", "regular review"],
            "static_references": ["dashboard provides", "comprehensive view"],
            "non_specific_metrics": ["current performance", "strategic insights"]
        }
        
        for category, phrases in hardcoded_phrases.items():
            for phrase in phrases:
                if phrase in text_lower:
                    indicators.append(f"{category}: '{phrase}'")
        
        return indicators
    
    def _analyze_data_consistency(self):
        """Analyze data consistency across different time periods"""
        logger.info("Analyzing data consistency...")
        
        consistency_analysis = {
            "file_groups": {},
            "consistency_issues": [],
            "data_evolution_patterns": {},
            "anomalies_detected": []
        }
        
        # Group files by type
        file_groups = {
            "business_formation": list(self.json_output_dir.glob("business_formation_*.json")),
            "property_market": list(self.json_output_dir.glob("property_market_*.json")),
            "economic_indicators": list(self.json_output_dir.glob("economic_indicators_*.json")),
            "government_spending": list(self.json_output_dir.glob("government_spending_*.json")),
            "risk_assessment": list(self.json_output_dir.glob("risk_assessment_*.json"))
        }
        
        for group_name, files in file_groups.items():
            if len(files) > 1:
                group_consistency = self._analyze_group_consistency(files)
                consistency_analysis["file_groups"][group_name] = group_consistency
        
        self.probe_results["data_consistency_analysis"] = consistency_analysis
    
    def _analyze_group_consistency(self, files: List[Path]) -> Dict[str, Any]:
        """Analyze consistency within a group of files"""
        group_analysis = {
            "total_files": len(files),
            "structure_consistency": True,
            "data_volume_changes": [],
            "chart_consistency": {},
            "issues_found": []
        }
        
        file_data = []
        
        for file_path in files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    file_data.append({
                        "file": file_path.name,
                        "data": data,
                        "timestamp": data.get("timestamp", "unknown")
                    })
            except Exception as e:
                group_analysis["issues_found"].append(f"Error reading {file_path.name}: {e}")
        
        # Sort by timestamp
        file_data.sort(key=lambda x: x["timestamp"])
        
        # Check structure consistency
        if len(file_data) > 1:
            first_structure = self._get_file_structure(file_data[0]["data"])
            for file_info in file_data[1:]:
                current_structure = self._get_file_structure(file_info["data"])
                if first_structure != current_structure:
                    group_analysis["structure_consistency"] = False
                    group_analysis["issues_found"].append(
                        f"Structure mismatch in {file_info['file']}"
                    )
        
        return group_analysis
    
    def _get_file_structure(self, data: Dict) -> Dict[str, Any]:
        """Get the structure signature of a file"""
        structure = {
            "top_level_keys": sorted(data.keys()),
            "content_type": type(data.get("content", {})).__name__
        }
        
        content = data.get("content", {})
        if isinstance(content, dict) and "charts" in content:
            charts = content["charts"]
            structure["chart_names"] = sorted(charts.keys()) if isinstance(charts, dict) else "not_dict"
        
        return structure
    
    def _identify_missing_data_patterns(self):
        """Identify patterns in missing data"""
        logger.info("Identifying missing data patterns...")
        
        missing_patterns = {
            "empty_charts_by_type": {},
            "missing_fields_frequency": {},
            "data_availability_timeline": {},
            "critical_missing_data": []
        }
        
        json_files = list(self.json_output_dir.glob("*.json"))
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                # Analyze missing data in this file
                file_missing_data = self._analyze_file_missing_data(data)
                
                # Aggregate patterns
                for chart_type, count in file_missing_data.get("empty_charts_by_type", {}).items():
                    missing_patterns["empty_charts_by_type"][chart_type] = \
                        missing_patterns["empty_charts_by_type"].get(chart_type, 0) + count
                
                for field in file_missing_data.get("missing_fields", []):
                    missing_patterns["missing_fields_frequency"][field] = \
                        missing_patterns["missing_fields_frequency"].get(field, 0) + 1
                
            except Exception as e:
                logger.error(f"Error analyzing missing data in {json_file.name}: {e}")
        
        self.probe_results["missing_data_patterns"] = missing_patterns
    
    def _analyze_file_missing_data(self, data: Dict) -> Dict[str, Any]:
        """Analyze missing data in a single file"""
        missing_data = {
            "empty_charts_by_type": {},
            "missing_fields": [],
            "data_completeness_score": 0.0
        }
        
        charts = data.get("content", {}).get("charts", {})
        total_charts = len(charts)
        charts_with_data = 0
        
        for chart_name, chart_data in charts.items():
            chart_type = chart_data.get("chart_type", "unknown")
            
            # Check if chart has data
            has_data = not self._is_chart_data_empty(chart_data.get("data", {}))
            
            if has_data:
                charts_with_data += 1
            else:
                missing_data["empty_charts_by_type"][chart_type] = \
                    missing_data["empty_charts_by_type"].get(chart_type, 0) + 1
            
            # Check for missing required fields
            required_fields = ["title", "chart_type", "data"]
            for field in required_fields:
                if field not in chart_data:
                    missing_data["missing_fields"].append(f"{chart_name}.{field}")
        
        # Calculate completeness score
        if total_charts > 0:
            missing_data["data_completeness_score"] = charts_with_data / total_charts
        
        return missing_data
    
    def _is_chart_data_empty(self, chart_data: Dict) -> bool:
        """Check if chart data is empty"""
        if not chart_data:
            return True
        
        for key, value in chart_data.items():
            if isinstance(value, list) and len(value) > 0:
                return False
            elif isinstance(value, (int, float)) and value != 0:
                return False
            elif isinstance(value, str) and value.strip():
                return False
        
        return True
    
    def _save_probe_results(self):
        """Save detailed probe results"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = self.json_output_dir / f"detailed_probe_results_{timestamp}.json"
        
        try:
            with open(results_file, 'w') as f:
                json.dump(self.probe_results, f, indent=2, default=str)
            
            logger.info(f"Detailed probe results saved to: {results_file}")
            
            # Create executive summary
            self._create_executive_summary(timestamp)
            
        except Exception as e:
            logger.error(f"Error saving probe results: {e}")
    
    def _create_executive_summary(self, timestamp: str):
        """Create executive summary of findings"""
        summary_file = self.json_output_dir / f"executive_summary_{timestamp}.txt"
        
        try:
            with open(summary_file, 'w') as f:
                f.write("EXECUTIVE SUMMARY - DETAILED DATA PROBE\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Analysis Date: {self.probe_results['timestamp']}\n\n")
                
                # Key Findings
                f.write("🔍 KEY FINDINGS\n")
                f.write("-" * 15 + "\n")
                
                # Economic indicators issues
                econ_analysis = self.probe_results.get("economic_indicators_structure_analysis", {})
                structure_issues = len(econ_analysis.get("structure_issues", []))
                f.write(f"• Economic indicators structure issues: {structure_issues} files\n")
                
                # Data truncation
                truncation_analysis = self.probe_results.get("data_truncation_detailed_analysis", {})
                truncated_files = len(truncation_analysis.get("truncation_limits_detected", {}))
                f.write(f"• Files with data truncation: {truncated_files}\n")
                
                # Hardcoded insights
                insights_analysis = self.probe_results.get("hardcoded_insights_detailed_analysis", {})
                hardcoded_examples = len(insights_analysis.get("hardcoded_content_examples", []))
                f.write(f"• Hardcoded insight examples found: {hardcoded_examples}\n")
                
                # Missing data patterns
                missing_analysis = self.probe_results.get("missing_data_patterns", {})
                empty_chart_types = len(missing_analysis.get("empty_charts_by_type", {}))
                f.write(f"• Chart types with missing data: {empty_chart_types}\n")
                
                f.write("\n🎯 PRIORITY ACTIONS\n")
                f.write("-" * 17 + "\n")
                f.write("1. Fix economic indicators data structure (list vs dict issue)\n")
                f.write("2. Remove data truncation limits (15, 20 item limits detected)\n")
                f.write("3. Replace hardcoded insights with dynamic LLM generation\n")
                f.write("4. Implement comprehensive data validation pipeline\n")
                f.write("5. Add real-time data quality monitoring\n")
                
            logger.info(f"Executive summary saved to: {summary_file}")
            
        except Exception as e:
            logger.error(f"Error creating executive summary: {e}")

def main():
    """Main function"""
    print("🔬 Starting Detailed Data Probe...")
    
    probe = DetailedDataProbe()
    results = probe.run_detailed_probe()
    
    print("\n🎯 DETAILED PROBE SUMMARY")
    print("=" * 35)
    
    # Economic indicators analysis
    econ_analysis = results.get("economic_indicators_structure_analysis", {})
    print(f"📊 Economic indicators files analyzed: {econ_analysis.get('total_files', 0)}")
    print(f"⚠️  Structure issues detected: {len(econ_analysis.get('structure_issues', []))}")
    
    # Truncation analysis
    truncation_analysis = results.get("data_truncation_detailed_analysis", {})
    truncated_files = len(truncation_analysis.get("truncation_limits_detected", {}))
    print(f"✂️  Files with truncation: {truncated_files}")
    
    # Insights analysis
    insights_analysis = results.get("hardcoded_insights_detailed_analysis", {})
    hardcoded_examples = len(insights_analysis.get("hardcoded_content_examples", []))
    print(f"🤖 Hardcoded insight examples: {hardcoded_examples}")
    
    print("\n✅ Detailed probe complete! Check dashboard_json_output/ for full reports.")

if __name__ == "__main__":
    main()