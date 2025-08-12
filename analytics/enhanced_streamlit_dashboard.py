#!/usr/bin/env python3
"""
Enhanced Streamlit Dashboard for Economic Intelligence Platform
Integrates visual intelligence capabilities with comprehensive charts and analytics
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
from datetime import datetime, timedelta
import sys
from pathlib import Path
import logging
from typing import Dict, List, Any, Optional

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

# Import our custom modules
from enhanced_economic_intelligence import EnhancedEconomicIntelligencePlatform
from enhanced_visual_intelligence import EnhancedVisualIntelligencePlatform
from chart_generator import InteractiveChartGenerator, create_dashboard_charts
from silver_data_connector import SilverLayerConnector, DataSourceConfig
from llm_config import LLMConfig, LLMProvider, get_default_config, create_llm_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Enhanced Economic Intelligence Dashboard - Singapore",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for enhanced styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        padding: 1rem;
        background: linear-gradient(90deg, #f0f2f6, #ffffff);
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
    }
    
    .insight-card {
        background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%);
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid #90caf9;
        margin: 1rem 0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .insight-card h4 {
        color: #1565c0;
        margin-bottom: 1rem;
        font-weight: 600;
        font-size: 1.1rem;
    }
    
    .insight-card ul {
        margin: 0;
        padding-left: 1.2rem;
    }
    
    .insight-card li {
        margin-bottom: 0.5rem;
        line-height: 1.5;
        color: #2c3e50;
    }
    
    .warning-card {
        background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%);
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid #ffeaa7;
        margin: 1rem 0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .success-card {
        background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid #c3e6cb;
        margin: 1rem 0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .success-card h4 {
        color: #155724;
        margin-bottom: 1rem;
        font-weight: 600;
        font-size: 1.1rem;
    }
    
    .success-card ul {
        margin: 0;
        padding-left: 1.2rem;
    }
    
    .success-card li {
        margin-bottom: 0.5rem;
        line-height: 1.5;
        color: #2c3e50;
    }
    
    .chart-container {
        background: white;
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.12);
        margin: 1.5rem 0;
        border: 1px solid #e1e5e9;
    }
    
    .stMetric {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 1rem;
        border-radius: 10px;
        border: 1px solid #dee2e6;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    .stSubheader {
        color: #2c3e50;
        font-weight: 600;
        margin-top: 2rem;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e9ecef;
    }
    
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    /* Enhanced plotly chart styling */
    .js-plotly-plot {
        border-radius: 8px;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)

class EnhancedDashboard:
    """Enhanced dashboard with comprehensive visualizations"""
    
    def __init__(self):
        self.data_config = DataSourceConfig()
        self.llm_config = get_default_config()
        self.data_connector = None
        self.visual_platform = None
        self.charts_data = None
        self.page_json_data = {}
        self.json_output_dir = "./dashboard_json_output"
        self._ensure_json_output_dir()
    
    def _ensure_json_output_dir(self):
        """Ensure JSON output directory exists"""
        os.makedirs(self.json_output_dir, exist_ok=True)
    
    def _save_page_json(self, page_name: str, content_data: Dict[str, Any]):
        """Save page content as JSON"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{page_name}_{timestamp}.json"
            filepath = os.path.join(self.json_output_dir, filename)
            
            page_json = {
                "page_name": page_name,
                "timestamp": timestamp,
                "content": content_data,
                "metadata": {
                    "dashboard_version": "2.0",
                    "generated_by": "Enhanced Economic Intelligence Dashboard"
                }
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(page_json, f, indent=2, default=str)
            
            # Also store in session state for display
            if 'page_json_files' not in st.session_state:
                st.session_state.page_json_files = []
            st.session_state.page_json_files.append(filepath)
            
            logger.info(f"Saved page JSON: {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving page JSON: {e}")
    
    def _save_debug_json(self, debug_name: str, debug_info: Dict[str, Any]):
        """Save debug information as JSON file"""
        try:
            # Create output directory if it doesn't exist
            output_dir = Path("./dashboard_json_output/debug")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{debug_name}_{timestamp}.json"
            filepath = output_dir / filename
            
            # Prepare JSON data
            json_data = {
                "debug_name": debug_name,
                "timestamp": timestamp,
                "debug_info": debug_info,
                "metadata": {
                    "dashboard_version": "2.0",
                    "debug_type": "system_diagnostics"
                }
            }
            
            # Save to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False, default=str)
                
            logger.info(f"Saved debug info {debug_name} to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving debug info {debug_name}: {e}")
    
    def _comprehensive_json_analysis(self):
        """Perform comprehensive analysis of all JSON outputs with enhanced probes"""
        analysis_results = {
            "timestamp": datetime.now().isoformat(),
            "total_files_analyzed": 0,
            "files_with_issues": [],
            "common_issues": [],
            "recommendations": [],
            "data_quality_summary": {},
            "chart_analysis": {},
            "data_truncation_summary": {
                "files_with_truncation_indicators": 0,
                "total_suspicious_patterns": 0,
                "high_confidence_truncation": [],
                "truncation_details": []
            },
            "file_size_analysis": {},
            "data_consistency_issues": [],
            "performance_metrics": {
                "largest_files": [],
                "smallest_files": [],
                "average_file_size": 0,
                "total_data_points": 0
            },
            "data_source_health": {},
            "enhancement_opportunities": []
        }
        
        try:
            file_sizes = []
            total_data_points = 0
            
            # Analyze all JSON files in output directory
            for filename in os.listdir(self.json_output_dir):
                if filename.endswith('.json'):
                    filepath = os.path.join(self.json_output_dir, filename)
                    analysis_results["total_files_analyzed"] += 1
                    
                    try:
                        # Get file size for analysis
                        file_size = os.path.getsize(filepath)
                        file_sizes.append((filename, file_size))
                        analysis_results["file_size_analysis"][filename] = {
                            "size_bytes": file_size,
                            "size_mb": round(file_size / (1024 * 1024), 2)
                        }
                        
                        with open(filepath, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        # Validate JSON structure
                        validation_result = self._validate_json_structure(data, ["page_name", "content"])
                        if not validation_result["is_valid"]:
                            analysis_results["files_with_issues"].append({
                                "filename": filename,
                                "issues": validation_result["missing_fields"] + validation_result["empty_fields"] + validation_result["malformed_fields"],
                                "validation_details": validation_result
                            })
                        
                        # Analyze chart data if present
                        if "charts" in data.get("content", {}):
                            chart_analysis = self._analyze_chart_data(data["content"]["charts"], filename)
                            analysis_results["chart_analysis"][filename] = chart_analysis
                            
                            # Check for data truncation indicators
                            if chart_analysis.get("data_truncation_indicators") or chart_analysis.get("suspicious_patterns"):
                                analysis_results["data_truncation_summary"]["files_with_truncation_indicators"] += 1
                                analysis_results["data_truncation_summary"]["total_suspicious_patterns"] += len(chart_analysis.get("suspicious_patterns", []))
                                
                                # Record truncation details
                                for indicator in chart_analysis.get("data_truncation_indicators", []):
                                    analysis_results["data_truncation_summary"]["truncation_details"].append({
                                        "file": filename,
                                        "chart": indicator["chart"],
                                        "data_points": indicator["data_points"],
                                        "reason": indicator["reason"]
                                    })
                                
                                if chart_analysis.get("truncation_assessment", {}).get("confidence_level") == "High":
                                    analysis_results["data_truncation_summary"]["high_confidence_truncation"].append(filename)
                            
                            # Count total data points
                            for chart_name, data_points in chart_analysis.get("data_volume_analysis", {}).items():
                                total_data_points += data_points
                        
                        # Analyze data source health
                        if "data_summary" in data.get("content", {}):
                            data_summary = data["content"]["data_summary"]
                            analysis_results["data_source_health"][filename] = {
                                "total_records": data_summary.get("total_records", 0),
                                "data_quality_score": data_summary.get("data_quality_score", 0),
                                "data_sources": data_summary.get("data_sources", []),
                                "individual_source_counts": data_summary.get("individual_source_counts", {})
                            }
                            
                    except Exception as e:
                        analysis_results["files_with_issues"].append({
                            "filename": filename,
                            "issues": [f"JSON parsing error: {str(e)}"],
                            "error_type": type(e).__name__
                        })
            
            # Calculate performance metrics
            if file_sizes:
                file_sizes.sort(key=lambda x: x[1], reverse=True)
                analysis_results["performance_metrics"]["largest_files"] = file_sizes[:3]
                analysis_results["performance_metrics"]["smallest_files"] = file_sizes[-3:]
                analysis_results["performance_metrics"]["average_file_size"] = sum(size for _, size in file_sizes) / len(file_sizes)
            
            analysis_results["performance_metrics"]["total_data_points"] = total_data_points
            
            # Identify enhancement opportunities
            if analysis_results["data_truncation_summary"]["files_with_truncation_indicators"] > 0:
                analysis_results["enhancement_opportunities"].append({
                    "type": "Data Completeness",
                    "description": f"Remove data limits to access complete datasets - {analysis_results['data_truncation_summary']['files_with_truncation_indicators']} files show truncation indicators",
                    "priority": "High",
                    "impact": "Significantly improved data analysis accuracy"
                })
            
            if total_data_points < 50000:  # Arbitrary threshold for "low" data volume
                analysis_results["enhancement_opportunities"].append({
                    "type": "Data Volume",
                    "description": f"Current total data points ({total_data_points:,}) suggest potential for richer analysis with more comprehensive data loading",
                    "priority": "Medium",
                    "impact": "Enhanced analytical depth and insights"
                })
            
            # Generate recommendations based on analysis
            analysis_results["recommendations"] = self._generate_improvement_recommendations(analysis_results)
            
            # Save comprehensive analysis
            self._save_debug_json("comprehensive_json_analysis", analysis_results)
            
            return analysis_results
            
        except Exception as e:
            logger.error(f"Error in comprehensive JSON analysis: {e}", exc_info=True)
            return analysis_results
    
    def _analyze_chart_data(self, charts: Dict[str, Any], filename: str) -> Dict[str, Any]:
        """Analyze chart data for issues and quality with enhanced data truncation detection"""
        chart_analysis = {
            "total_charts": len(charts),
            "charts_with_data": 0,
            "charts_without_data": 0,
            "missing_chart_types": [],
            "data_quality_issues": [],
            "data_truncation_indicators": [],
            "data_volume_analysis": {},
            "potential_data_limits": [],
            "suspicious_patterns": []
        }
        
        for chart_name, chart_info in charts.items():
            if isinstance(chart_info, dict):
                # Check for data availability
                data_summary = chart_info.get("data_summary", {})
                data_points = data_summary.get("data_points", 0)
                
                # Record data volume for analysis
                chart_analysis["data_volume_analysis"][chart_name] = data_points
                
                if data_points > 0:
                    chart_analysis["charts_with_data"] += 1
                    
                    # Check for potential data truncation (suspicious round numbers)
                    if data_points in [100, 500, 1000, 5000, 10000]:
                        chart_analysis["data_truncation_indicators"].append({
                            "chart": chart_name,
                            "data_points": data_points,
                            "reason": "Suspicious round number suggesting potential data limit",
                            "filename": filename
                        })
                    
                    # Check for exactly 1000 records (common default limit)
                    if data_points == 1000:
                        chart_analysis["suspicious_patterns"].append({
                            "chart": chart_name,
                            "pattern": "Exactly 1000 records",
                            "likelihood": "High probability of data truncation",
                            "recommendation": "Remove data limits to load complete dataset"
                        })
                else:
                    chart_analysis["charts_without_data"] += 1
                    chart_analysis["data_quality_issues"].append(f"{chart_name}: No data points")
                
                # Check for missing chart types
                if not chart_info.get("chart_type") or chart_info.get("chart_type") == "":
                    chart_analysis["missing_chart_types"].append(chart_name)
                
                # Check metadata for explicit limits
                metadata = chart_info.get("metadata", {})
                if isinstance(metadata, dict):
                    limit_indicators = [k for k in metadata.keys() if "limit" in k.lower() or "max" in k.lower() or "truncat" in k.lower()]
                    if limit_indicators:
                        chart_analysis["potential_data_limits"].append({
                            "chart": chart_name,
                            "limit_metadata": {k: metadata[k] for k in limit_indicators},
                            "filename": filename
                        })
        
        # Add overall assessment
        total_suspicious = len(chart_analysis["data_truncation_indicators"]) + len(chart_analysis["suspicious_patterns"])
        if total_suspicious > 0:
            chart_analysis["truncation_assessment"] = {
                "likely_truncated": total_suspicious > chart_analysis["total_charts"] * 0.3,  # 30% threshold
                "confidence_level": "High" if total_suspicious > chart_analysis["total_charts"] * 0.5 else "Medium",
                "recommendation": "Review data loading limits and remove artificial constraints"
            }
        
        return chart_analysis
    
    def _generate_improvement_recommendations(self, analysis_results: Dict[str, Any]) -> List[str]:
        """Generate comprehensive recommendations based on enhanced analysis results"""
        recommendations = []
        
        # Priority 1: Data Truncation Issues
        truncation_summary = analysis_results.get("data_truncation_summary", {})
        if truncation_summary.get("files_with_truncation_indicators", 0) > 0:
            recommendations.append(f"🚨 CRITICAL: Remove data limits from {truncation_summary['files_with_truncation_indicators']} files showing truncation indicators")
            recommendations.append(f"📊 Data Impact: {truncation_summary['total_suspicious_patterns']} suspicious patterns detected - likely missing significant data")
            
            if truncation_summary.get("high_confidence_truncation"):
                high_conf_files = ", ".join(truncation_summary["high_confidence_truncation"])
                recommendations.append(f"⚠️ High confidence truncation detected in: {high_conf_files}")
        
        # Priority 2: Data Volume Analysis
        performance_metrics = analysis_results.get("performance_metrics", {})
        total_data_points = performance_metrics.get("total_data_points", 0)
        if total_data_points > 0:
            recommendations.append(f"📈 Current data volume: {total_data_points:,} total data points across all visualizations")
            if total_data_points < 50000:
                recommendations.append("💡 Consider loading complete datasets for richer analysis (current volume suggests truncation)")
        
        # Priority 3: File-specific Issues
        if analysis_results["files_with_issues"]:
            recommendations.append(f"🔧 Fix {len(analysis_results['files_with_issues'])} JSON files with validation issues")
            
            # Detailed issue breakdown
            error_types = {}
            for file_issue in analysis_results["files_with_issues"]:
                error_type = file_issue.get("error_type", "validation_error")
                error_types[error_type] = error_types.get(error_type, 0) + 1
            
            for error_type, count in error_types.items():
                recommendations.append(f"  - {error_type}: {count} files affected")
        
        # Priority 4: Chart Analysis
        chart_issues = {"missing_types": 0, "no_data": 0}
        for filename, chart_data in analysis_results["chart_analysis"].items():
            if chart_data.get("missing_chart_types"):
                chart_issues["missing_types"] += len(chart_data["missing_chart_types"])
                recommendations.append(f"📊 Add chart_type mapping for {len(chart_data['missing_chart_types'])} charts in {filename}")
            
            if chart_data.get("charts_without_data", 0) > 0:
                chart_issues["no_data"] += chart_data["charts_without_data"]
                recommendations.append(f"🔍 Investigate {chart_data['charts_without_data']} charts without data in {filename}")
        
        # Priority 5: Enhancement Opportunities
        for opportunity in analysis_results.get("enhancement_opportunities", []):
            priority_icon = "🚨" if opportunity["priority"] == "High" else "💡" if opportunity["priority"] == "Medium" else "📝"
            recommendations.append(f"{priority_icon} {opportunity['type']}: {opportunity['description']}")
        
        # Priority 6: Data Source Health
        data_source_health = analysis_results.get("data_source_health", {})
        if data_source_health:
            low_quality_files = [filename for filename, health in data_source_health.items() 
                               if health.get("data_quality_score", 0) < 0.7]
            if low_quality_files:
                recommendations.append(f"⚠️ Data quality concerns in {len(low_quality_files)} files: {', '.join(low_quality_files)}")
        
        # Priority 7: Performance Optimization
        file_size_analysis = analysis_results.get("file_size_analysis", {})
        if file_size_analysis:
            large_files = [(f, data["size_mb"]) for f, data in file_size_analysis.items() if data["size_mb"] > 10]
            if large_files:
                recommendations.append(f"🗂️ Consider optimizing {len(large_files)} large files (>10MB) for better performance")
        
        # Priority 8: General System Improvements
        if analysis_results["total_files_analyzed"] > 0:
            recommendations.append("🔄 Implement automated JSON validation in CI/CD pipeline")
            recommendations.append("📊 Add real-time data quality monitoring dashboard")
            recommendations.append("🏥 Create comprehensive data source health checks")
            recommendations.append("📈 Implement data completeness tracking and alerting")
            recommendations.append("🔍 Add automated detection of data truncation patterns")
        
        return recommendations

    def _validate_json_structure(self, json_data: Dict[str, Any], expected_fields: List[str]) -> Dict[str, Any]:
        """Validate JSON structure and identify missing or malformed fields"""
        validation_result = {
            "is_valid": True,
            "missing_fields": [],
            "empty_fields": [],
            "malformed_fields": [],
            "field_types": {},
            "validation_timestamp": datetime.now().isoformat()
        }
        
        for field in expected_fields:
            if field not in json_data:
                validation_result["missing_fields"].append(field)
                validation_result["is_valid"] = False
            else:
                field_value = json_data[field]
                validation_result["field_types"][field] = type(field_value).__name__
                
                # Check for empty fields
                if field_value == "" or field_value == [] or field_value == {}:
                    validation_result["empty_fields"].append(field)
                    validation_result["is_valid"] = False
                
                # Check for malformed chart data
                if field == "charts" and isinstance(field_value, dict):
                    for chart_name, chart_data in field_value.items():
                        if isinstance(chart_data, dict):
                            if chart_data.get("chart_type", "") == "":
                                validation_result["malformed_fields"].append(f"charts.{chart_name}.chart_type")
                                validation_result["is_valid"] = False
        
        return validation_result
    
    def _analyze_data_quality(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze data quality metrics"""
        quality_metrics = {
            "timestamp": datetime.now().isoformat(),
            "total_data_sources": len(data) if data else 0,
            "data_source_health": {},
            "overall_quality_score": 0.0,
            "quality_issues": [],
            "recommendations": []
        }
        
        if not data:
            quality_metrics["quality_issues"].append("No data available")
            return quality_metrics
        
        total_score = 0
        for source_name, source_data in data.items():
            source_health = {
                "record_count": len(source_data) if hasattr(source_data, '__len__') else 0,
                "has_data": bool(source_data is not None and len(source_data) > 0 if hasattr(source_data, '__len__') else source_data is not None),
                "data_type": type(source_data).__name__,
                "quality_score": 0.0
            }
            
            # Calculate quality score
            if source_health["has_data"]:
                source_health["quality_score"] += 50  # Base score for having data
                if source_health["record_count"] > 100:
                    source_health["quality_score"] += 30  # Good volume
                elif source_health["record_count"] > 10:
                    source_health["quality_score"] += 15  # Moderate volume
                
                if source_health["record_count"] > 0:
                    source_health["quality_score"] += 20  # Data completeness
            
            quality_metrics["data_source_health"][source_name] = source_health
            total_score += source_health["quality_score"]
            
            # Add recommendations
            if source_health["record_count"] < 10:
                quality_metrics["quality_issues"].append(f"Low data volume in {source_name}")
                quality_metrics["recommendations"].append(f"Increase data collection for {source_name}")
        
        quality_metrics["overall_quality_score"] = total_score / len(data) if data else 0
        
        return quality_metrics
    
    def _track_performance_metrics(self, operation_name: str, start_time: datetime, end_time: datetime, additional_metrics: Dict[str, Any] = None) -> Dict[str, Any]:
        """Track performance metrics for operations"""
        duration = (end_time - start_time).total_seconds()
        
        performance_data = {
            "operation": operation_name,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "status": "completed",
            "performance_rating": "excellent" if duration < 1 else "good" if duration < 5 else "slow"
        }
        
        if additional_metrics:
            performance_data.update(additional_metrics)
        
        return performance_data
    
    def _calculate_data_points(self, chart_data: Any) -> int:
        """Calculate the number of data points in chart data for various chart types"""
        if not chart_data:
            return 0
        
        # Handle case where chart_data is a list (multi-series charts)
        if isinstance(chart_data, list):
            total_points = 0
            for series in chart_data:
                if isinstance(series, dict):
                    if 'x' in series and 'y' in series:
                        x_data = series['x']
                        y_data = series['y']
                        if isinstance(x_data, list) and isinstance(y_data, list):
                            total_points += min(len(x_data), len(y_data))
            return total_points
        
        # Handle case where chart_data is a dict
        if not isinstance(chart_data, dict):
            return 0
        
        # For pie charts with 'labels' and 'values'
        if 'labels' in chart_data and 'values' in chart_data:
            labels = chart_data['labels']
            values = chart_data['values']
            if isinstance(labels, list) and isinstance(values, list):
                return min(len(labels), len(values))
        
        # For bar/line charts with 'x' and 'y'
        if 'x' in chart_data and 'y' in chart_data:
            x_data = chart_data['x']
            y_data = chart_data['y']
            if isinstance(x_data, list) and isinstance(y_data, list):
                return min(len(x_data), len(y_data))
        
        # For other chart types, count non-empty values
        total_points = 0
        for key, value in chart_data.items():
            if isinstance(value, list):
                total_points += len(value)
            elif value is not None:
                total_points += 1
        
        return total_points
    
    def _extract_chart_data(self, visual_report: Dict[str, Any], section: str) -> Dict[str, Any]:
        """Extract complete chart data from visual report for JSON output with enhanced validation"""
        chart_data = {}
        
        # Default chart type mapping based on common patterns
        chart_type_mapping = {
            'distribution': 'pie',
            'trend': 'line',
            'comparison': 'bar',
            'correlation': 'scatter',
            'timeline': 'line',
            'status': 'pie',
            'indicators': 'bar',
            'spending': 'bar',
            'market': 'line',
            'health': 'gauge',
            'diversity': 'gauge',
            'coverage': 'bar'
        }
        
        if 'visualizations' in visual_report and section in visual_report['visualizations']:
            charts = visual_report['visualizations'][section]
            
            # Handle both dict and list structures
            if isinstance(charts, dict):
                for chart_name, chart_info in charts.items():
                    if isinstance(chart_info, dict):
                        # Determine chart type if missing
                        chart_type = chart_info.get('type', chart_info.get('chart_type', ''))
                        if not chart_type:
                            for keyword, default_type in chart_type_mapping.items():
                                if keyword in chart_name.lower():
                                    chart_type = default_type
                                    break
                            if not chart_type:
                                chart_type = 'bar'  # Default fallback
                        
                        # Extract complete chart data including actual values
                        chart_data[chart_name] = {
                            "title": chart_info.get('title', chart_name.replace('_', ' ').title()),
                            "chart_type": chart_type,
                            "description": chart_info.get('description', ''),
                            "data": chart_info.get('data', {}),  # Include actual chart data
                            "data_summary": {
                                "data_points": self._calculate_data_points(chart_info.get('data', {})),
                                "categories": list(chart_info.get('data', {}).keys()) if isinstance(chart_info.get('data'), dict) else [],
                                "has_valid_data": bool(chart_info.get('data') and len(chart_info.get('data', {})) > 0)
                            },
                            "metadata": {
                                "last_updated": datetime.now().isoformat(),
                                "data_source": section,
                                "chart_status": "active" if chart_info.get('data') else "no_data"
                            }
                        }
                    else:
                        # Handle case where chart_info is not a dict
                        chart_data[chart_name] = {
                            "title": str(chart_info) if chart_info else chart_name.replace('_', ' ').title(),
                            "chart_type": 'unknown',
                            "data_summary": {
                                "data_points": 0,
                                "categories": [],
                                "has_valid_data": False
                            },
                            "metadata": {
                                "last_updated": datetime.now().isoformat(),
                                "data_source": section,
                                "chart_status": "error"
                            }
                        }
            elif isinstance(charts, list):
                # Handle case where charts is a list
                for i, chart_info in enumerate(charts):
                    chart_name = f"chart_{i+1}"
                    if isinstance(chart_info, dict):
                        chart_type = chart_info.get('chart_type', 'bar')
                        chart_data[chart_name] = {
                            "title": chart_info.get('title', f'Chart {i+1}'),
                            "chart_type": chart_type,
                            "data_summary": {
                                "data_points": len(chart_info.get('data', {}).get('x', [])) if isinstance(chart_info.get('data'), dict) else 0,
                                "categories": list(chart_info.get('data', {}).keys()) if isinstance(chart_info.get('data'), dict) else [],
                                "has_valid_data": bool(chart_info.get('data'))
                            },
                            "metadata": {
                                "last_updated": datetime.now().isoformat(),
                                "data_source": section,
                                "chart_status": "active" if chart_info.get('data') else "no_data"
                            }
                        }
                    else:
                        chart_data[chart_name] = {
                            "title": f'Chart {i+1}',
                            "chart_type": 'unknown',
                            "data_summary": {
                                "data_points": 0,
                                "categories": [],
                                "has_valid_data": False
                            },
                            "metadata": {
                                "last_updated": datetime.now().isoformat(),
                                "data_source": section,
                                "chart_status": "error"
                            }
                        }
        
        # Validate chart data structure
        validation_result = self._validate_json_structure(
            {"charts": chart_data}, 
            ["charts"]
        )
        
        # Add validation metadata
        for chart_name in chart_data:
            chart_data[chart_name]["validation"] = {
                "is_valid": chart_name not in [field.split('.')[-1] for field in validation_result.get("malformed_fields", [])],
                "validation_timestamp": validation_result["validation_timestamp"]
            }
        
        return chart_data
        
    @st.cache_data(ttl=3600)  # Cache for 1 hour
    def load_data(_self):
        """Load and cache data from all sources"""
        debug_info = {
            "timestamp": datetime.now().isoformat(),
            "step": "data_loading",
            "status": "starting"
        }
        
        try:
            _self.data_connector = SilverLayerConnector(_self.data_config)
            debug_info["connector_created"] = True
            debug_info["connector_connected"] = _self.data_connector.is_connected()
            
            # Load all data sources individually since load_all_data doesn't exist
            # Load all available data from silver layer (no limits for comprehensive analysis)
            data = {
                'acra_companies': _self.data_connector.load_acra_companies(),
                'economic_indicators': _self.data_connector.load_economic_indicators(),
                'government_expenditure': _self.data_connector.load_government_expenditure(),
                'property_market': _self.data_connector.load_property_market(),
                'commercial_rental': _self.data_connector.load_commercial_rental()
            }
            
            debug_info["data_loaded"] = True
            debug_info["data_sources"] = {k: len(v) for k, v in data.items()}
            debug_info["total_records"] = sum(len(v) for v in data.values())
            debug_info["status"] = "success"
            
            # Save debug info
            _self._save_debug_json("data_loading_debug", debug_info)
            
            return data
            
        except Exception as e:
            debug_info["status"] = "error"
            debug_info["error"] = str(e)
            debug_info["error_type"] = type(e).__name__
            
            # Save debug info
            _self._save_debug_json("data_loading_debug", debug_info)
            
            st.error(f"Error loading data: {e}")
            logger.error(f"Data loading error: {e}", exc_info=True)
            return {}
    
    @st.cache_data(ttl=1800)  # Cache for 30 minutes
    def generate_visual_intelligence(_self):
        """Generate visual intelligence report"""
        debug_info = {
            "timestamp": datetime.now().isoformat(),
            "step": "visual_intelligence_generation",
            "status": "starting"
        }
        
        try:
            _self.visual_platform = EnhancedVisualIntelligencePlatform(_self.data_config, _self.llm_config)
            debug_info["platform_created"] = True
            debug_info["llm_config"] = {
                "provider": _self.llm_config.provider.value if _self.llm_config else "None",
                "model_name": _self.llm_config.model_name if _self.llm_config else "None",
                "api_base": _self.llm_config.api_base if _self.llm_config else "None",
                "available": hasattr(_self.llm_config, 'provider') if _self.llm_config else False
            }
            
            report = _self.visual_platform.generate_enhanced_visual_report()
            debug_info["report_generated"] = True
            debug_info["report_keys"] = list(report.keys()) if report else []
            debug_info["visualizations_count"] = len(report.get('visualizations', {})) if report else 0
            debug_info["status"] = "success"
            
            # Save debug info
            _self._save_debug_json("visual_intelligence_debug", debug_info)
            
            return report
            
        except Exception as e:
            debug_info["status"] = "error"
            debug_info["error"] = str(e)
            debug_info["error_type"] = type(e).__name__
            
            # Save debug info
            _self._save_debug_json("visual_intelligence_debug", debug_info)
            
            st.error(f"Error generating visual intelligence: {e}")
            logger.error(f"Visual intelligence generation error: {e}", exc_info=True)
            return {}
    
    def render_header(self):
        """Render dashboard header"""
        st.markdown('<div class="main-header">📊 Enhanced Economic Intelligence Dashboard - Singapore</div>', unsafe_allow_html=True)
        
        # Status indicators
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Data Sources", "4", "Active")
        with col2:
            st.metric("Last Updated", datetime.now().strftime("%H:%M"), "Real-time")
        with col3:
            st.metric("Report Status", "Enhanced", "Visual Intelligence")
        with col4:
            st.metric("Dashboard Version", "2.0", "Advanced Analytics")
    
    def render_executive_summary(self, data: Dict[str, pd.DataFrame], visual_report: Dict[str, Any]):
        """Render executive summary with key metrics"""
        st.header("📈 Executive Summary")
        
        # Key Performance Indicators
        if 'visualizations' in visual_report and 'executive_metrics' in visual_report['visualizations']:
            exec_charts = visual_report['visualizations']['executive_metrics']
            
            if 'kpi_dashboard' in exec_charts:
                kpis = exec_charts['kpi_dashboard']['data']
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    if 'Total Companies' in kpis:
                        kpi = kpis['Total Companies']
                        st.metric(
                            "Total Companies",
                            f"{kpi['value']:,}",
                            delta=f"📈 {kpi['trend']}"
                        )
                
                with col2:
                    if 'Economic Indicators' in kpis:
                        kpi = kpis['Economic Indicators']
                        st.metric(
                            "Economic Indicators",
                            f"{kpi['value']}",
                            delta=f"📊 {kpi['trend']}"
                        )
                
                with col3:
                    if 'Government Spending' in kpis:
                        kpi = kpis['Government Spending']
                        st.metric(
                            "Gov Spending (M SGD)",
                            f"{kpi['value']:,.1f}",
                            delta=f"💰 {kpi['trend']}"
                        )
                
                with col4:
                    if 'Property Records' in kpis:
                        kpi = kpis['Property Records']
                        st.metric(
                            "Property Records",
                            f"{kpi['value']:,}",
                            delta=f"🏠 {kpi['trend']}"
                        )
        
        # Economic Health Score
        if 'visualizations' in visual_report and 'executive_metrics' in visual_report['visualizations']:
            exec_charts = visual_report['visualizations']['executive_metrics']
            
            if 'health_summary' in exec_charts:
                health_data = exec_charts['health_summary']['data']
                
                # Create gauge chart for economic health
                fig = go.Figure(go.Indicator(
                    mode = "gauge+number+delta",
                    value = health_data['value'],
                    domain = {'x': [0, 1], 'y': [0, 1]},
                    title = {'text': "Economic Health Score"},
                    delta = {'reference': 70},
                    gauge = {
                        'axis': {'range': [None, 100]},
                        'bar': {'color': "darkblue"},
                        'steps': [
                            {'range': [0, 30], 'color': "lightgray"},
                            {'range': [30, 70], 'color': "yellow"},
                            {'range': [70, 100], 'color': "green"}
                        ],
                        'threshold': {
                            'line': {'color': "red", 'width': 4},
                            'thickness': 0.75,
                            'value': 90
                        }
                    }
                ))
                
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
    
    def render_business_formation_analysis(self, visual_report: Dict[str, Any]):
        """Render enhanced business formation analysis with comprehensive insights"""
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.header("🏢 Singapore Business Formation Intelligence")
        
        if 'visualizations' not in visual_report or 'business_formation' not in visual_report['visualizations']:
            st.warning("Business formation data not available")
            return
        
        charts = visual_report['visualizations']['business_formation']
        
        # Prepare JSON data
        page_content = {
            "charts": self._extract_chart_data(visual_report, 'business_formation'),
            "visualizations_available": list(charts.keys()),
            "section": "business_formation",
            "enhanced_features": {
                "total_charts": len(charts),
                "chart_types": list(set([chart.get('type', 'unknown') for chart in charts.values()])),
                "data_coverage": "comprehensive"
            }
        }
        
        # Save JSON
        self._save_page_json("business_formation", page_content)
        
        # Key Metrics Overview
        if 'data_quality_overview' in charts:
            metrics_data = charts['data_quality_overview']['data']
            st.subheader("📊 Business Formation Overview")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Companies", f"{metrics_data['total_companies']:,}")
            with col2:
                st.metric("Unique Entities", f"{metrics_data['unique_entities']:,}")
            with col3:
                entity_completeness = metrics_data['data_completeness'].get('entity_type', 0)
                st.metric("Data Quality", f"{entity_completeness:.1f}%")
            with col4:
                postal_completeness = metrics_data['data_completeness'].get('postal_code', 0)
                st.metric("Location Coverage", f"{postal_completeness:.1f}%")
        
        # Geographic Analysis Section
        st.subheader("🗺️ Geographic Distribution Analysis")
        geo_col1, geo_col2 = st.columns([2, 1])
        
        with geo_col1:
            if 'geographic_distribution' in charts:
                chart_data = charts['geographic_distribution']
                fig = px.pie(
                    values=chart_data['data']['values'],
                    names=chart_data['data']['labels'],
                    title=chart_data['title'],
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig.update_traces(
                    textposition='inside', 
                    textinfo='percent+label',
                    hovertemplate='<b>%{label}</b><br>Companies: %{value}<br>Percentage: %{percent}<extra></extra>'
                )
                fig.update_layout(height=500, showlegend=True, legend=dict(orientation="v", x=1.05))
                st.plotly_chart(fig, use_container_width=True)
        
        with geo_col2:
            if 'regional_business_density' in charts:
                density_data = charts['regional_business_density']['data']
                st.markdown("**Regional Business Metrics**")
                
                # Create a summary table
                density_df = pd.DataFrame({
                    'Region': density_data['regions'][:10],
                    'Companies': density_data['total_companies'][:10],
                    'Diversity': density_data['entity_diversity'][:10],
                    'Active': density_data['active_companies'][:10]
                })
                st.dataframe(density_df, use_container_width=True, height=400)
        
        # Entity Analysis Section
        st.subheader("🏛️ Business Entity Analysis")
        entity_col1, entity_col2 = st.columns(2)
        
        with entity_col1:
            if 'entity_type_distribution' in charts:
                chart_data = charts['entity_type_distribution']
                fig = px.bar(
                    x=chart_data['data']['y'],
                    y=chart_data['data']['x'],
                    orientation='h',
                    title=chart_data['title'],
                    color=chart_data['data']['y'],
                    color_continuous_scale='viridis'
                )
                fig.update_layout(
                    xaxis_title="Number of Companies", 
                    yaxis_title="Entity Type",
                    height=400,
                    showlegend=False
                )
                fig.update_traces(hovertemplate='<b>%{y}</b><br>Companies: %{x:,}<extra></extra>')
                st.plotly_chart(fig, use_container_width=True)
        
        with entity_col2:
            if 'entity_performance_analysis' in charts:
                chart_data = charts['entity_performance_analysis']
                fig = px.bar(
                    x=chart_data['data']['x'],
                    y=chart_data['data']['y'],
                    orientation='h',
                    title=chart_data['title'],
                    color=chart_data['data']['x'],
                    color_continuous_scale='RdYlGn'
                )
                fig.update_layout(
                    xaxis_title="Success Rate (%)", 
                    yaxis_title="Entity Type",
                    height=400,
                    showlegend=False
                )
                fig.update_traces(hovertemplate='<b>%{y}</b><br>Success Rate: %{x:.1f}%<extra></extra>')
                st.plotly_chart(fig, use_container_width=True)
        
        # Status Analysis
        if 'company_status' in charts:
            st.subheader("📈 Company Status Distribution")
            chart_data = charts['company_status']
            fig = px.pie(
                values=chart_data['data']['values'],
                names=chart_data['data']['labels'],
                title=chart_data['title'],
                hole=0.4,
                color_discrete_sequence=['#ff6b6b', '#4ecdc4']
            )
            fig.update_traces(
                textposition='inside', 
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>Companies: %{value:,}<br>Percentage: %{percent}<extra></extra>'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Temporal Analysis Section
        st.subheader("📅 Temporal Business Formation Trends")
        
        # Registration trends
        if 'registration_trends' in charts:
            chart_data = charts['registration_trends']
            fig = px.line(
                x=chart_data['data']['x'],
                y=chart_data['data']['y'],
                title=chart_data['title'],
                line_shape='spline'
            )
            fig.update_layout(
                xaxis_title="Period", 
                yaxis_title="Number of Registrations",
                height=400,
                hovermode='x unified'
            )
            fig.update_traces(
                line=dict(width=3, color='#1f77b4'),
                hovertemplate='<b>%{x}</b><br>Registrations: %{y:,}<extra></extra>'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Advanced Analytics Section
        st.subheader("🔬 Advanced Business Formation Analytics")
        adv_col1, adv_col2 = st.columns(2)
        
        with adv_col1:
            # Yearly trends
            if 'yearly_registration_growth' in charts:
                chart_data = charts['yearly_registration_growth']
                fig = px.bar(
                    x=chart_data['data']['x'],
                    y=chart_data['data']['y'],
                    title=chart_data['title'],
                    color=chart_data['data']['y'],
                    color_continuous_scale='blues'
                )
                fig.update_layout(
                    xaxis_title="Year", 
                    yaxis_title="Registrations",
                    height=400,
                    showlegend=False
                )
                fig.update_traces(hovertemplate='<b>%{x}</b><br>Registrations: %{y:,}<extra></extra>')
                st.plotly_chart(fig, use_container_width=True)
        
        with adv_col2:
            # Seasonal patterns
            if 'seasonal_registration_patterns' in charts:
                chart_data = charts['seasonal_registration_patterns']
                fig = go.Figure()
                fig.add_trace(go.Scatterpolar(
                    r=chart_data['data']['values'],
                    theta=chart_data['data']['labels'],
                    fill='toself',
                    name='Registrations',
                    line_color='rgb(32, 201, 151)'
                ))
                fig.update_layout(
                    polar=dict(
                        radialaxis=dict(visible=True, range=[0, max(chart_data['data']['values'])])
                    ),
                    title=chart_data['title'],
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Recent velocity analysis
        if 'recent_formation_velocity' in charts:
            st.subheader("⚡ Recent Business Formation Velocity")
            chart_data = charts['recent_formation_velocity']
            fig = px.area(
                x=chart_data['data']['x'],
                y=chart_data['data']['y'],
                title=chart_data['title'],
                color_discrete_sequence=['rgba(31, 119, 180, 0.7)']
            )
            fig.update_layout(
                xaxis_title="Period", 
                yaxis_title="Registrations",
                height=400,
                hovermode='x unified'
            )
            fig.update_traces(hovertemplate='<b>%{x}</b><br>Registrations: %{y:,}<extra></extra>')
            st.plotly_chart(fig, use_container_width=True)
        
        # Insights and Recommendations
        st.subheader("💡 Key Insights & Recommendations")
        
        insights_col1, insights_col2 = st.columns(2)
        
        with insights_col1:
            st.markdown("""
            <div class="insight-card">
                <h4>🎯 Strategic Insights</h4>
                <ul>
                    <li><strong>Geographic Concentration:</strong> Business formation shows clear regional preferences</li>
                    <li><strong>Entity Preferences:</strong> Local companies dominate the business landscape</li>
                    <li><strong>Seasonal Patterns:</strong> Registration activity varies throughout the year</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
        
        with insights_col2:
            st.markdown("""
            <div class="success-card">
                <h4>📈 Growth Opportunities</h4>
                <ul>
                    <li><strong>Underserved Regions:</strong> Potential for business development in lower-density areas</li>
                    <li><strong>Entity Diversification:</strong> Opportunities for alternative business structures</li>
                    <li><strong>Timing Optimization:</strong> Strategic registration timing based on seasonal trends</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    def render_economic_indicators(self, visual_report: Dict[str, Any]):
        """Render economic indicators charts"""
        st.header("📊 Economic Indicators Dashboard")
        
        if 'visualizations' not in visual_report or 'economic_indicators' not in visual_report['visualizations']:
            st.warning("Economic indicators data not available")
            return
        
        charts = visual_report['visualizations']['economic_indicators']
        
        # Prepare JSON data
        page_content = {
            "charts": self._extract_chart_data(visual_report, 'economic_indicators'),
            "visualizations_available": list(charts.keys()),
            "section": "economic_indicators"
        }
        
        # Save JSON
        self._save_page_json("economic_indicators", page_content)
        
        # Key Indicators
        if 'key_indicators' in charts:
            chart_data = charts['key_indicators']
            fig = px.bar(
                x=chart_data['data']['x'],
                y=chart_data['data']['y'],
                orientation='h',
                title=chart_data['title']
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Indicator Categories
            if 'indicator_categories' in charts:
                chart_data = charts['indicator_categories']
                fig = px.bar(
                    x=chart_data['data']['x'],
                    y=chart_data['data']['y'],
                    title=chart_data['title']
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Economic Trends
            if 'economic_trends' in charts:
                chart_data = charts['economic_trends']
                fig = go.Figure()
                
                for series in chart_data['data']:
                    fig.add_trace(go.Scatter(
                        x=series['x'],
                        y=series['y'],
                        mode='lines+markers',
                        name=series['name']
                    ))
                
                fig.update_layout(title=chart_data['title'])
                st.plotly_chart(fig, use_container_width=True)
    
    def render_government_spending(self, visual_report: Dict[str, Any]):
        """Render government spending analysis"""
        st.header("💰 Government Spending Analysis")
        
        if 'visualizations' not in visual_report or 'government_spending' not in visual_report['visualizations']:
            st.warning("Government spending data not available")
            return
        
        charts = visual_report['visualizations']['government_spending']
        
        # Prepare JSON data
        page_content = {
            "charts": self._extract_chart_data(visual_report, 'government_spending'),
            "visualizations_available": list(charts.keys()),
            "section": "government_spending"
        }
        
        # Save JSON
        self._save_page_json("government_spending", page_content)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Spending by Category
            if 'spending_by_category' in charts:
                chart_data = charts['spending_by_category']
                fig = px.pie(
                    values=chart_data['data']['values'],
                    names=chart_data['data']['labels'],
                    title=chart_data['title']
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Spending Trends
            if 'spending_trends' in charts:
                chart_data = charts['spending_trends']
                fig = px.line(
                    x=chart_data['data']['x'],
                    y=chart_data['data']['y'],
                    title=chart_data['title']
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Top Spending Areas
        if 'top_spending_areas' in charts:
            chart_data = charts['top_spending_areas']
            fig = px.bar(
                x=chart_data['data']['x'],
                y=chart_data['data']['y'],
                orientation='h',
                title=chart_data['title']
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    def render_property_market(self, visual_report: Dict[str, Any]):
        """Render property market analysis"""
        st.header("🏠 Property Market Intelligence")
        
        if 'visualizations' not in visual_report or 'property_market' not in visual_report['visualizations']:
            st.warning("Property market data not available")
            return
        
        charts = visual_report['visualizations']['property_market']
        
        # Prepare JSON data
        page_content = {
            "charts": self._extract_chart_data(visual_report, 'property_market'),
            "visualizations_available": list(charts.keys()),
            "section": "property_market"
        }
        
        # Save JSON
        self._save_page_json("property_market", page_content)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Rental Distribution
            if 'rental_distribution' in charts:
                chart_data = charts['rental_distribution']
                fig = px.histogram(
                    x=chart_data['data']['x'],
                    nbins=chart_data['data']['nbins'],
                    title=chart_data['title']
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Property Types
            if 'property_types' in charts:
                chart_data = charts['property_types']
                fig = px.pie(
                    values=chart_data['data']['values'],
                    names=chart_data['data']['labels'],
                    title=chart_data['title'],
                    hole=0.4
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # District Analysis
        if 'district_rentals' in charts:
            chart_data = charts['district_rentals']
            fig = px.bar(
                x=chart_data['data']['x'],
                y=chart_data['data']['y'],
                title=chart_data['title']
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
        # Price Ranges
        if 'price_ranges' in charts:
            chart_data = charts['price_ranges']
            fig = px.bar(
                x=chart_data['data']['x'],
                y=chart_data['data']['y'],
                title=chart_data['title']
            )
            st.plotly_chart(fig, use_container_width=True)
    
    def render_cross_sector_analysis(self, visual_report: Dict[str, Any]):
        """Render cross-sector correlation analysis"""
        st.header("🔗 Cross-Sector Analysis")
        
        if 'visualizations' not in visual_report or 'cross_sector' not in visual_report['visualizations']:
            st.warning("Cross-sector analysis data not available")
            return
        
        charts = visual_report['visualizations']['cross_sector']
        
        # Prepare JSON data
        page_content = {
            "charts": self._extract_chart_data(visual_report, 'cross_sector'),
            "visualizations_available": list(charts.keys()),
            "section": "cross_sector"
        }
        
        # Save JSON
        self._save_page_json("cross_sector_analysis", page_content)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Data Coverage
            if 'data_coverage' in charts:
                chart_data = charts['data_coverage']
                fig = px.bar(
                    x=chart_data['data']['x'],
                    y=chart_data['data']['y'],
                    title=chart_data['title']
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Economic Diversity
            if 'economic_diversity' in charts:
                chart_data = charts['economic_diversity']
                fig = go.Figure(go.Indicator(
                    mode = "gauge+number",
                    value = chart_data['data']['value'],
                    domain = {'x': [0, 1], 'y': [0, 1]},
                    title = {'text': chart_data['title']},
                    gauge = {
                        'axis': {'range': [None, chart_data['data']['max']]},
                        'bar': {'color': "darkblue"},
                        'steps': [
                            {'range': [0, 30], 'color': "lightgray"},
                            {'range': [30, 70], 'color': "yellow"},
                            {'range': [70, 100], 'color': "green"}
                        ]
                    }
                ))
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
        
        # Sector Health Scorecard
        if 'sector_health' in charts:
            chart_data = charts['sector_health']
            fig = go.Figure()
            
            fig.add_trace(go.Scatterpolar(
                r=chart_data['data']['values'],
                theta=chart_data['data']['categories'],
                fill='toself',
                name='Sector Health'
            ))
            
            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 100]
                    )
                ),
                title=chart_data['title']
            )
            st.plotly_chart(fig, use_container_width=True)
    
    def render_risk_assessment(self, visual_report: Dict[str, Any]):
        """Render risk assessment visualizations"""
        st.header("⚠️ Risk Assessment Dashboard")
        
        if 'visualizations' not in visual_report or 'risk_assessment' not in visual_report['visualizations']:
            st.warning("Risk assessment data not available")
            return
        
        charts = visual_report['visualizations']['risk_assessment']
        
        # Prepare JSON data
        page_content = {
            "charts": self._extract_chart_data(visual_report, 'risk_assessment'),
            "visualizations_available": list(charts.keys()),
            "section": "risk_assessment"
        }
        
        # Save JSON
        self._save_page_json("risk_assessment", page_content)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Risk Distribution
            if 'risk_distribution' in charts:
                chart_data = charts['risk_distribution']
                fig = px.pie(
                    values=chart_data['data']['values'],
                    names=chart_data['data']['labels'],
                    title=chart_data['title'],
                    color_discrete_map={
                        'Low Risk': 'green',
                        'Medium Risk': 'yellow',
                        'High Risk': 'red'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Risk Trends
            if 'risk_trends' in charts:
                chart_data = charts['risk_trends']
                fig = px.line(
                    x=chart_data['data']['x'],
                    y=chart_data['data']['y'],
                    title=chart_data['title']
                )
                fig.update_traces(line_color='red')
                st.plotly_chart(fig, use_container_width=True)
    
    def render_insights_and_recommendations(self, visual_report: Dict[str, Any]):
        """Render insights and recommendations"""
        st.header("💡 Visual Insights & Recommendations")
        
        # Prepare JSON data
        page_content = {
            "visual_insights": visual_report.get('visual_insights', []),
            "chart_recommendations": visual_report.get('chart_recommendations', []),
            "section": "insights_recommendations",
            "insights_count": len(visual_report.get('visual_insights', [])),
            "recommendations_count": len(visual_report.get('chart_recommendations', []))
        }
        
        # Save JSON
        self._save_page_json("insights_recommendations", page_content)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Key Insights")
            
            if 'visual_insights' in visual_report:
                for insight in visual_report['visual_insights']:
                    st.markdown(f"""
                    <div class="insight-card">
                        <h4>🎯 {insight['category']}</h4>
                        <p>{insight['insight']}</p>
                        <small><strong>Chart Reference:</strong> {insight['chart_reference']}</small>
                    </div>
                    """, unsafe_allow_html=True)
        
        with col2:
            st.subheader("🎯 Recommendations")
            
            if 'chart_recommendations' in visual_report:
                for rec in visual_report['chart_recommendations']:
                    priority_color = {
                        'High': 'success-card',
                        'Medium': 'warning-card',
                        'Low': 'insight-card'
                    }.get(rec['priority'], 'insight-card')
                    
                    st.markdown(f"""
                    <div class="{priority_color}">
                        <h4>📋 {rec['category']} ({rec['priority']} Priority)</h4>
                        <p><strong>Recommendation:</strong> {rec['recommendation']}</p>
                        <p><strong>Implementation:</strong> {rec['implementation']}</p>
                    </div>
                    """, unsafe_allow_html=True)
    
    def render_sidebar(self, visual_report: Dict[str, Any]):
        """Render sidebar with controls and information"""
        st.sidebar.title("📊 Dashboard Controls")
        
        # Report Information
        st.sidebar.subheader("📋 Report Information")
        if visual_report:
            st.sidebar.info(f"""
            **Report Type:** {visual_report.get('report_type', 'Unknown')}
            
            **Generated:** {visual_report.get('report_timestamp', 'Unknown')}
            
            **Visualizations:** {visual_report.get('visualization_count', 0)}
            
            **LLM Enabled:** {visual_report.get('llm_enabled', False)}
            """)
        
        # Data Source Status
        st.sidebar.subheader("🔌 Data Sources")
        if 'data_sources_status' in visual_report:
            for source, status in visual_report['data_sources_status'].items():
                status_icon = "✅" if status else "❌"
                st.sidebar.text(f"{status_icon} {source.replace('_', ' ').title()}")
        
        # Refresh Controls
        st.sidebar.subheader("🔄 Controls")
        if st.sidebar.button("🔄 Refresh Data", type="primary"):
            st.cache_data.clear()
            st.rerun()
        
        if st.sidebar.button("📊 Regenerate Report"):
            st.cache_data.clear()
            st.rerun()
        
        # JSON Output Tracking
        st.sidebar.subheader("📄 JSON Output Tracking")
        if 'page_json_files' in st.session_state and st.session_state.page_json_files:
            st.sidebar.info(f"Generated {len(st.session_state.page_json_files)} JSON files")
            
            # Show latest JSON files
            latest_files = st.session_state.page_json_files[-5:]  # Show last 5 files
            for filepath in latest_files:
                filename = os.path.basename(filepath)
                if os.path.exists(filepath):
                    with open(filepath, 'r') as f:
                        json_content = f.read()
                    st.sidebar.download_button(
                        label=f"📥 {filename}",
                        data=json_content,
                        file_name=filename,
                        mime="application/json",
                        key=f"download_{filename}"
                    )
        else:
            st.sidebar.info("No JSON files generated yet")
        
        # Export Options
        st.sidebar.subheader("📤 Export Options")
        if visual_report:
            report_json = json.dumps(visual_report, indent=2, default=str)
            st.sidebar.download_button(
                label="📄 Download Report (JSON)",
                data=report_json,
                file_name=f"enhanced_visual_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
    
    def run(self):
        """Run the enhanced dashboard"""
        start_time = datetime.now()  # Track dashboard generation start time
        run_debug = {
            "timestamp": start_time.isoformat(),
            "step": "dashboard_run",
            "status": "starting"
        }
        
        try:
            # Load data
            with st.spinner("Loading economic data..."):
                data = self.load_data()
                run_debug["data_loading_completed"] = True
                run_debug["data_available"] = data is not None and len(data) > 0
            
            # Generate visual intelligence report
            with st.spinner("Generating enhanced visual intelligence..."):
                visual_report = self.generate_visual_intelligence()
                run_debug["visual_report_completed"] = True
                run_debug["visual_report_available"] = visual_report is not None and len(visual_report) > 0
            
            # Render sidebar
            self.render_sidebar(visual_report)
            run_debug["sidebar_rendered"] = True
            
            # Render main dashboard
            self.render_header()
            run_debug["header_rendered"] = True
            
            if visual_report:
                # Analyze data quality
                data_quality_analysis = self._analyze_data_quality(data)
                
                # Capture main dashboard overview with enhanced debugging
                dashboard_overview = {
                    "data_summary": {
                        "total_sources": len(data) if isinstance(data, dict) else 0,
                        "data_sources": list(data.keys()) if isinstance(data, dict) else [],
                        "total_records": sum(len(df) for df in data.values()) if isinstance(data, dict) else 0,
                        "data_type": str(type(data).__name__) if data is not None else "None",
                        "individual_source_counts": {k: len(v) for k, v in data.items()} if isinstance(data, dict) else {},
                        "data_quality_score": data_quality_analysis.get('overall_quality_score', 0)
                    },
                    "visual_report_sections": list(visual_report.get('visualizations', {}).keys()),
                    "report_timestamp": visual_report.get('report_timestamp', ''),
                    "visualization_count": visual_report.get('visualization_count', 0),
                    "llm_enabled": visual_report.get('llm_enabled', False),
                    "data_sources_status": {
                        "database_connection": True,
                        "llm_connection": visual_report.get('llm_enabled', False),
                        "data_availability": {
                            "acra_companies": bool(data.get('acra_companies') is not None and len(data.get('acra_companies', [])) > 0) if isinstance(data, dict) else False,
                            "economic_indicators": bool(data.get('economic_indicators') is not None and len(data.get('economic_indicators', [])) > 0) if isinstance(data, dict) else False,
                            "government_expenditure": bool(data.get('government_expenditure') is not None and len(data.get('government_expenditure', [])) > 0) if isinstance(data, dict) else False,
                            "property_market": bool(data.get('property_market') is not None and len(data.get('property_market', [])) > 0) if isinstance(data, dict) else False,
                            "commercial_rental": bool(data.get('commercial_rental') is not None and len(data.get('commercial_rental', [])) > 0) if isinstance(data, dict) else False
                        },
                        "data_quality_issues": data_quality_analysis.get('quality_issues', []),
                        "data_recommendations": data_quality_analysis.get('recommendations', [])
                    },
                    "system_health": {
                        "overall_status": "healthy" if data_quality_analysis.get('overall_quality_score', 0) > 70 else "warning" if data_quality_analysis.get('overall_quality_score', 0) > 40 else "critical",
                        "last_health_check": datetime.now().isoformat(),
                        "performance_metrics": self._track_performance_metrics("dashboard_generation", start_time, datetime.now(), {
                            "data_sources_loaded": len(data) if isinstance(data, dict) else 0,
                            "visualizations_generated": visual_report.get('visualization_count', 0),
                            "total_records_processed": sum(len(df) for df in data.values()) if isinstance(data, dict) else 0
                        })
                    }
                }
                self._save_page_json("dashboard_overview", dashboard_overview)
                run_debug["dashboard_overview_saved"] = True
                
                # Executive Summary
                self.render_executive_summary(data, visual_report)
                run_debug["executive_summary_rendered"] = True
                
                # Create tabs for different sections
                tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
                    "🏢 Business Formation",
                    "📊 Economic Indicators", 
                    "💰 Government Spending",
                    "🏠 Property Market",
                    "🔗 Cross-Sector Analysis",
                    "⚠️ Risk Assessment",
                    "💡 Insights & Recommendations"
                ])
                
                # Enhanced debugging for each tab
                tab_debug = {}
                
                with tab1:
                    try:
                        self.render_business_formation_analysis(visual_report)
                        run_debug["business_formation_tab_rendered"] = True
                        tab_debug["business_formation"] = {"status": "success", "charts_generated": True}
                    except Exception as e:
                        run_debug["business_formation_tab_error"] = str(e)
                        tab_debug["business_formation"] = {"status": "error", "error": str(e)}
                        logger.error(f"Business formation tab error: {e}", exc_info=True)
                
                with tab2:
                    try:
                        self.render_economic_indicators(visual_report)
                        run_debug["economic_indicators_tab_rendered"] = True
                        tab_debug["economic_indicators"] = {"status": "success", "charts_generated": True}
                    except Exception as e:
                        run_debug["economic_indicators_tab_error"] = str(e)
                        tab_debug["economic_indicators"] = {"status": "error", "error": str(e)}
                        logger.error(f"Economic indicators tab error: {e}", exc_info=True)
                
                with tab3:
                    try:
                        self.render_government_spending(visual_report)
                        run_debug["government_spending_tab_rendered"] = True
                        tab_debug["government_spending"] = {"status": "success", "charts_generated": True}
                    except Exception as e:
                        run_debug["government_spending_tab_error"] = str(e)
                        tab_debug["government_spending"] = {"status": "error", "error": str(e)}
                        logger.error(f"Government spending tab error: {e}", exc_info=True)
                
                with tab4:
                    try:
                        self.render_property_market(visual_report)
                        run_debug["property_market_tab_rendered"] = True
                        tab_debug["property_market"] = {"status": "success", "charts_generated": True}
                    except Exception as e:
                        run_debug["property_market_tab_error"] = str(e)
                        tab_debug["property_market"] = {"status": "error", "error": str(e)}
                        logger.error(f"Property market tab error: {e}", exc_info=True)
                
                with tab5:
                    try:
                        self.render_cross_sector_analysis(visual_report)
                        run_debug["cross_sector_analysis_tab_rendered"] = True
                        tab_debug["cross_sector_analysis"] = {"status": "success", "charts_generated": True}
                    except Exception as e:
                        run_debug["cross_sector_analysis_tab_error"] = str(e)
                        tab_debug["cross_sector_analysis"] = {"status": "error", "error": str(e)}
                        logger.error(f"Cross sector analysis tab error: {e}", exc_info=True)
                
                with tab6:
                    try:
                        self.render_risk_assessment(visual_report)
                        run_debug["risk_assessment_tab_rendered"] = True
                        tab_debug["risk_assessment"] = {"status": "success", "charts_generated": True}
                    except Exception as e:
                        run_debug["risk_assessment_tab_error"] = str(e)
                        tab_debug["risk_assessment"] = {"status": "error", "error": str(e)}
                        logger.error(f"Risk assessment tab error: {e}", exc_info=True)
                
                with tab7:
                    try:
                        self.render_insights_and_recommendations(visual_report)
                        run_debug["insights_recommendations_tab_rendered"] = True
                        tab_debug["insights_recommendations"] = {"status": "success", "charts_generated": True}
                    except Exception as e:
                        run_debug["insights_recommendations_tab_error"] = str(e)
                        tab_debug["insights_recommendations"] = {"status": "error", "error": str(e)}
                        logger.error(f"Insights recommendations tab error: {e}", exc_info=True)
                
                # Save detailed tab debugging information
                run_debug["tab_details"] = tab_debug
                run_debug["successful_tabs"] = sum(1 for tab in tab_debug.values() if tab.get("status") == "success")
                run_debug["failed_tabs"] = sum(1 for tab in tab_debug.values() if tab.get("status") == "error")
                run_debug["all_tabs_rendered"] = True
                run_debug["status"] = "success"
            
            else:
                st.error("Failed to generate visual intelligence report")
                st.info("Please check data connections and try refreshing the dashboard.")
                run_debug["visual_report_warning_shown"] = True
                run_debug["status"] = "partial_success"
            
            # Perform comprehensive JSON analysis
            logger.info("Performing comprehensive JSON analysis...")
            analysis_results = self._comprehensive_json_analysis()
            run_debug["json_analysis"] = {
                "files_analyzed": analysis_results["total_files_analyzed"],
                "files_with_issues": len(analysis_results["files_with_issues"]),
                "total_recommendations": len(analysis_results["recommendations"])
            }
                
            # Save final run debug info
            self._save_debug_json("dashboard_run_debug", run_debug)
            
            logger.info(f"JSON Analysis: {analysis_results['total_files_analyzed']} files analyzed, {len(analysis_results['files_with_issues'])} with issues")
                
        except Exception as e:
            run_debug["status"] = "error"
            run_debug["error"] = str(e)
            run_debug["error_type"] = type(e).__name__
            
            # Save error debug info
            self._save_debug_json("dashboard_run_debug", run_debug)
            
            st.error(f"Dashboard error: {e}")
            logger.error(f"Dashboard error: {e}", exc_info=True)

def main():
    """Main function to run the enhanced dashboard"""
    dashboard = EnhancedDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()