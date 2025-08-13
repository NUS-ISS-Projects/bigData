#!/usr/bin/env python3
"""
Text Truncation Detector for Business Formation Dashboard

This script identifies and analyzes text truncation issues in:
1. Dashboard display code (Streamlit components)
2. JSON output files
3. LLM-generated content
4. Data processing pipelines

Focus: Business Formation tab AI insights and recommendations
"""

import os
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TextTruncationDetector:
    def __init__(self, base_dir: str = None):
        self.base_dir = Path(base_dir) if base_dir else Path.cwd()
        self.json_output_dir = self.base_dir / "dashboard_json_output"
        self.analysis_results = {
            "timestamp": datetime.now().isoformat(),
            "truncation_issues": [],
            "code_analysis": {},
            "json_analysis": {},
            "recommendations": [],
            "summary": {}
        }
        
    def detect_code_truncation_patterns(self) -> Dict[str, Any]:
        """Detect text truncation patterns in Python code files"""
        logger.info("Analyzing code files for truncation patterns...")
        
        code_files = [
            "enhanced_streamlit_dashboard.py",
            "streamlit_dashboard.py",
            "llm_analysis_engine.py",
            "enhanced_economic_intelligence.py"
        ]
        
        truncation_patterns = [
            r'\[:500\]',  # [:500] slicing
            r'\[:300\]',  # [:300] slicing
            r'\[:1000\]', # [:1000] slicing
            r'\.\.\.\s*if\s+len\(',  # ... if len( pattern
            r'truncate\(',  # truncate function calls
            r'limit\s*=\s*\d+',  # limit= parameters
            r'max_length\s*=\s*\d+',  # max_length parameters
            r'substring\(',  # substring function calls
            r'\[:\d+\]',  # general [:number] slicing
        ]
        
        code_analysis = {
            "files_analyzed": 0,
            "truncation_instances": [],
            "suspicious_patterns": [],
            "files_with_issues": []
        }
        
        for file_name in code_files:
            file_path = self.base_dir / file_name
            if not file_path.exists():
                continue
                
            code_analysis["files_analyzed"] += 1
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    lines = content.split('\n')
                    
                for line_num, line in enumerate(lines, 1):
                    for pattern in truncation_patterns:
                        matches = re.finditer(pattern, line, re.IGNORECASE)
                        for match in matches:
                            truncation_info = {
                                "file": file_name,
                                "line_number": line_num,
                                "pattern": pattern,
                                "matched_text": match.group(),
                                "full_line": line.strip(),
                                "severity": self._assess_truncation_severity(line, match.group())
                            }
                            code_analysis["truncation_instances"].append(truncation_info)
                            
                            if file_name not in code_analysis["files_with_issues"]:
                                code_analysis["files_with_issues"].append(file_name)
                                
            except Exception as e:
                logger.error(f"Error analyzing {file_name}: {e}")
                
        return code_analysis
    
    def _assess_truncation_severity(self, line: str, match: str) -> str:
        """Assess the severity of a truncation pattern"""
        line_lower = line.lower()
        
        # High severity indicators
        if any(keyword in line_lower for keyword in ['llm_analysis', 'recommendations', 'insights', 'strategic']):
            return "HIGH"
        
        # Medium severity indicators
        if any(keyword in line_lower for keyword in ['content', 'text', 'display', 'render']):
            return "MEDIUM"
            
        # Low severity (might be intentional)
        if any(keyword in line_lower for keyword in ['preview', 'summary', 'title']):
            return "LOW"
            
        return "MEDIUM"
    
    def analyze_json_content_completeness(self) -> Dict[str, Any]:
        """Analyze JSON files for content completeness and potential truncation"""
        logger.info("Analyzing JSON files for content completeness...")
        
        json_analysis = {
            "files_analyzed": 0,
            "business_formation_files": [],
            "content_analysis": [],
            "truncation_indicators": [],
            "statistics": {}
        }
        
        if not self.json_output_dir.exists():
            logger.warning(f"JSON output directory not found: {self.json_output_dir}")
            return json_analysis
            
        # Focus on Business Formation files
        bf_files = list(self.json_output_dir.glob("business_formation*.json"))
        
        for json_file in bf_files:
            json_analysis["files_analyzed"] += 1
            json_analysis["business_formation_files"].append(json_file.name)
            
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                file_analysis = self._analyze_json_file_content(json_file.name, data)
                json_analysis["content_analysis"].append(file_analysis)
                
                # Check for truncation indicators
                truncation_indicators = self._detect_json_truncation_indicators(data)
                if truncation_indicators:
                    json_analysis["truncation_indicators"].extend(truncation_indicators)
                    
            except Exception as e:
                logger.error(f"Error analyzing {json_file}: {e}")
                
        # Calculate statistics
        json_analysis["statistics"] = self._calculate_json_statistics(json_analysis["content_analysis"])
        
        return json_analysis
    
    def _analyze_json_file_content(self, filename: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze individual JSON file content"""
        analysis = {
            "filename": filename,
            "file_type": "llm_insights" if "llm_insights" in filename else "regular",
            "content_metrics": {},
            "quality_indicators": [],
            "potential_issues": []
        }
        
        # Analyze content based on file type
        if "llm_insights" in filename:
            analysis["content_metrics"] = self._analyze_llm_content(data)
        else:
            analysis["content_metrics"] = self._analyze_regular_content(data)
            
        return analysis
    
    def _analyze_llm_content(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze LLM-generated content for completeness"""
        metrics = {
            "strategic_insights_length": 0,
            "recommendations_length": 0,
            "has_complete_insights": False,
            "has_complete_recommendations": False,
            "truncation_indicators": []
        }
        
        content = data.get("content", {})
        
        # Analyze strategic insights
        strategic_insights = content.get("strategic_insights", "")
        metrics["strategic_insights_length"] = len(strategic_insights)
        metrics["has_complete_insights"] = len(strategic_insights) > 500 and not strategic_insights.endswith("...")
        
        # Analyze recommendations
        recommendations = content.get("recommendations", "")
        metrics["recommendations_length"] = len(recommendations)
        metrics["has_complete_recommendations"] = len(recommendations) > 300 and not recommendations.endswith("...")
        
        # Check for truncation indicators
        for field_name, field_content in [("strategic_insights", strategic_insights), ("recommendations", recommendations)]:
            if self._has_truncation_indicators(field_content):
                metrics["truncation_indicators"].append(field_name)
                
        return metrics
    
    def _analyze_regular_content(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze regular dashboard content"""
        metrics = {
            "total_charts": 0,
            "charts_with_data": 0,
            "data_completeness_ratio": 0.0,
            "content_sections": []
        }
        
        # Analyze charts if available
        charts = data.get("charts", {})
        if charts:
            metrics["total_charts"] = len(charts)
            metrics["charts_with_data"] = len([k for k, v in charts.items() if v.get("data")])
            metrics["data_completeness_ratio"] = metrics["charts_with_data"] / metrics["total_charts"] if metrics["total_charts"] > 0 else 0.0
            
        # Identify content sections
        metrics["content_sections"] = list(data.keys())
        
        return metrics
    
    def _has_truncation_indicators(self, text: str) -> bool:
        """Check if text has indicators of being truncated"""
        if not text:
            return False
            
        truncation_indicators = [
            text.endswith("..."),
            text.endswith("…"),
            "[truncated]" in text.lower(),
            "[cut off]" in text.lower(),
            text.endswith(" t..."),  # Common pattern like "shift t..."
            text.endswith(" deve..."),  # Common pattern like "research and deve..."
            len(text) > 10 and text[-10:].count(" ") == 0 and text.endswith("...")  # Word cut off with ...
        ]
        
        return any(truncation_indicators)
    
    def _detect_json_truncation_indicators(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect truncation indicators in JSON data"""
        indicators = []
        
        def check_value(key_path: str, value: Any):
            if isinstance(value, str) and self._has_truncation_indicators(value):
                indicators.append({
                    "location": key_path,
                    "value_preview": value[-50:] if len(value) > 50 else value,
                    "truncation_type": "text_truncation",
                    "severity": "HIGH" if any(keyword in key_path.lower() for keyword in ['insights', 'recommendations', 'analysis']) else "MEDIUM"
                })
            elif isinstance(value, dict):
                for k, v in value.items():
                    check_value(f"{key_path}.{k}", v)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    check_value(f"{key_path}[{i}]", item)
        
        for key, value in data.items():
            check_value(key, value)
            
        return indicators
    
    def _calculate_json_statistics(self, content_analyses: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate overall statistics from JSON analyses"""
        stats = {
            "total_files": len(content_analyses),
            "llm_files": 0,
            "regular_files": 0,
            "avg_insights_length": 0,
            "avg_recommendations_length": 0,
            "files_with_complete_content": 0
        }
        
        insights_lengths = []
        recommendations_lengths = []
        
        for analysis in content_analyses:
            if analysis["file_type"] == "llm_insights":
                stats["llm_files"] += 1
                metrics = analysis["content_metrics"]
                
                insights_lengths.append(metrics.get("strategic_insights_length", 0))
                recommendations_lengths.append(metrics.get("recommendations_length", 0))
                
                if (metrics.get("has_complete_insights", False) and 
                    metrics.get("has_complete_recommendations", False)):
                    stats["files_with_complete_content"] += 1
            else:
                stats["regular_files"] += 1
                
        if insights_lengths:
            stats["avg_insights_length"] = sum(insights_lengths) / len(insights_lengths)
        if recommendations_lengths:
            stats["avg_recommendations_length"] = sum(recommendations_lengths) / len(recommendations_lengths)
            
        return stats
    
    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """Generate comprehensive truncation analysis report"""
        logger.info("Generating comprehensive truncation analysis report...")
        
        # Perform all analyses
        self.analysis_results["code_analysis"] = self.detect_code_truncation_patterns()
        self.analysis_results["json_analysis"] = self.analyze_json_content_completeness()
        
        # Generate recommendations
        self.analysis_results["recommendations"] = self._generate_recommendations()
        
        # Create summary
        self.analysis_results["summary"] = self._create_summary()
        
        # Save report
        report_filename = f"text_truncation_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        report_path = self.json_output_dir / report_filename
        
        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(self.analysis_results, f, indent=2, ensure_ascii=False)
            logger.info(f"Report saved: {report_path}")
        except Exception as e:
            logger.error(f"Error saving report: {e}")
            
        return self.analysis_results
    
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on analysis results"""
        recommendations = []
        
        code_analysis = self.analysis_results["code_analysis"]
        json_analysis = self.analysis_results["json_analysis"]
        
        # Code-based recommendations
        high_severity_issues = [issue for issue in code_analysis.get("truncation_instances", []) 
                               if issue["severity"] == "HIGH"]
        
        if high_severity_issues:
            recommendations.append(f"🚨 CRITICAL: Remove {len(high_severity_issues)} high-severity text truncations in code")
            for issue in high_severity_issues[:3]:  # Show top 3
                recommendations.append(f"   - {issue['file']}:{issue['line_number']} - {issue['matched_text']}")
                
        # JSON-based recommendations
        truncation_indicators = json_analysis.get("truncation_indicators", [])
        if truncation_indicators:
            high_severity_json = [ind for ind in truncation_indicators if ind["severity"] == "HIGH"]
            if high_severity_json:
                recommendations.append(f"⚠️ HIGH: {len(high_severity_json)} JSON content truncation indicators detected")
                
        # Content completeness recommendations
        stats = json_analysis.get("statistics", {})
        complete_files = stats.get("files_with_complete_content", 0)
        total_llm_files = stats.get("llm_files", 0)
        
        if total_llm_files > 0 and complete_files < total_llm_files:
            recommendations.append(f"📊 CONTENT: {total_llm_files - complete_files}/{total_llm_files} LLM files may have incomplete content")
            
        # Performance recommendations
        avg_insights = stats.get("avg_insights_length", 0)
        avg_recommendations = stats.get("avg_recommendations_length", 0)
        
        if avg_insights < 1000:
            recommendations.append(f"📝 INSIGHTS: Average insights length ({avg_insights:.0f} chars) suggests potential truncation")
        if avg_recommendations < 800:
            recommendations.append(f"💡 RECOMMENDATIONS: Average recommendations length ({avg_recommendations:.0f} chars) suggests potential truncation")
            
        if not recommendations:
            recommendations.append("✅ No significant text truncation issues detected")
            
        return recommendations
    
    def _create_summary(self) -> Dict[str, Any]:
        """Create analysis summary"""
        code_analysis = self.analysis_results["code_analysis"]
        json_analysis = self.analysis_results["json_analysis"]
        
        return {
            "total_code_files_analyzed": code_analysis.get("files_analyzed", 0),
            "total_json_files_analyzed": json_analysis.get("files_analyzed", 0),
            "code_truncation_instances": len(code_analysis.get("truncation_instances", [])),
            "json_truncation_indicators": len(json_analysis.get("truncation_indicators", [])),
            "high_severity_code_issues": len([i for i in code_analysis.get("truncation_instances", []) if i["severity"] == "HIGH"]),
            "high_severity_json_issues": len([i for i in json_analysis.get("truncation_indicators", []) if i["severity"] == "HIGH"]),
            "files_with_code_issues": len(code_analysis.get("files_with_issues", [])),
            "business_formation_files_found": len(json_analysis.get("business_formation_files", [])),
            "avg_insights_length": json_analysis.get("statistics", {}).get("avg_insights_length", 0),
            "avg_recommendations_length": json_analysis.get("statistics", {}).get("avg_recommendations_length", 0),
            "analysis_timestamp": self.analysis_results["timestamp"]
        }

def main():
    """Main execution function"""
    print("🔍 Text Truncation Detector - Business Formation Dashboard Analysis")
    print("=" * 70)
    
    detector = TextTruncationDetector()
    results = detector.generate_comprehensive_report()
    
    # Display summary
    summary = results["summary"]
    print(f"\n📊 ANALYSIS SUMMARY")
    print(f"📁 Code Files Analyzed: {summary['total_code_files_analyzed']}")
    print(f"📄 JSON Files Analyzed: {summary['total_json_files_analyzed']}")
    print(f"⚠️ Code Truncation Instances: {summary['code_truncation_instances']}")
    print(f"🚨 High Severity Code Issues: {summary['high_severity_code_issues']}")
    print(f"📊 JSON Truncation Indicators: {summary['json_truncation_indicators']}")
    print(f"🔥 High Severity JSON Issues: {summary['high_severity_json_issues']}")
    print(f"📈 Avg Insights Length: {summary['avg_insights_length']:.0f} characters")
    print(f"💡 Avg Recommendations Length: {summary['avg_recommendations_length']:.0f} characters")
    
    print(f"\n💡 RECOMMENDATIONS:")
    for i, rec in enumerate(results["recommendations"], 1):
        print(f"   {i}. {rec}")
    
    print(f"\n✅ Analysis complete. Detailed report saved to dashboard_json_output/")
    
if __name__ == "__main__":
    main()