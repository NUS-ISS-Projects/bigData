#!/usr/bin/env python3
"""
LLM-Based Economic Intelligence Analytics
Implements the Analytics & ML layer (Section D) using Large Language Models
Reads from Silver layer data to provide economic insights and analysis
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import pandas as pd
import numpy as np
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class EconomicInsight:
    """Data class for economic insights"""
    insight_type: str
    title: str
    description: str
    confidence_score: float
    data_sources: List[str]
    timestamp: datetime
    metadata: Dict[str, Any]

@dataclass
class AnomalyAlert:
    """Data class for anomaly detection alerts"""
    alert_type: str
    severity: str  # 'low', 'medium', 'high', 'critical'
    description: str
    affected_metrics: List[str]
    timestamp: datetime
    confidence_score: float
    recommended_actions: List[str]

class SilverDataLoader:
    """Loads and processes data from Silver layer"""
    
    def __init__(self, data_path: str = "/tmp/silver_data"):
        self.data_path = Path(data_path)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def load_acra_companies(self) -> pd.DataFrame:
        """Load ACRA companies data from silver layer"""
        try:
            # In a real implementation, this would connect to MinIO/Delta Lake
            # For now, we'll simulate with sample data structure
            sample_data = {
                'company_id': ['C001', 'C002', 'C003'],
                'company_name': ['Tech Innovations Pte Ltd', 'Green Energy Solutions', 'Financial Services Co'],
                'industry_category': ['Technology', 'Energy', 'Finance'],
                'registration_date': ['2023-01-15', '2023-02-20', '2023-03-10'],
                'status': ['Active', 'Active', 'Ceased'],
                'data_quality_score': [0.95, 0.88, 0.92]
            }
            df = pd.DataFrame(sample_data)
            df['registration_date'] = pd.to_datetime(df['registration_date'])
            self.logger.info(f"Loaded {len(df)} ACRA company records")
            return df
        except Exception as e:
            self.logger.error(f"Error loading ACRA data: {e}")
            return pd.DataFrame()
    
    def load_economic_indicators(self) -> pd.DataFrame:
        """Load SingStat economic indicators from silver layer"""
        try:
            sample_data = {
                'indicator_name': ['GDP Growth Rate', 'Inflation Rate', 'Unemployment Rate'],
                'value': [3.2, 2.1, 2.8],
                'period': ['2023-Q4', '2023-Q4', '2023-Q4'],
                'unit': ['%', '%', '%'],
                'data_quality_score': [0.98, 0.95, 0.93]
            }
            df = pd.DataFrame(sample_data)
            self.logger.info(f"Loaded {len(df)} economic indicator records")
            return df
        except Exception as e:
            self.logger.error(f"Error loading economic indicators: {e}")
            return pd.DataFrame()
    
    def load_government_expenditure(self) -> pd.DataFrame:
        """Load government expenditure data from silver layer"""
        try:
            sample_data = {
                'category': ['Infrastructure', 'Education', 'Healthcare'],
                'amount_million_sgd': [1500.0, 2200.0, 1800.0],
                'fiscal_year': ['2023', '2023', '2023'],
                'expenditure_type': ['Development', 'Operating', 'Operating'],
                'data_quality_score': [0.96, 0.94, 0.97]
            }
            df = pd.DataFrame(sample_data)
            self.logger.info(f"Loaded {len(df)} government expenditure records")
            return df
        except Exception as e:
            self.logger.error(f"Error loading government expenditure: {e}")
            return pd.DataFrame()
    
    def load_property_market(self) -> pd.DataFrame:
        """Load URA property market data from silver layer"""
        try:
            sample_data = {
                'district': ['Central', 'East', 'West'],
                'property_type': ['Condo', 'HDB', 'Landed'],
                'median_rental_psf': [4.2, 2.8, 3.5],
                'quarter': ['2023-Q4', '2023-Q4', '2023-Q4'],
                'data_quality_score': [0.91, 0.89, 0.93]
            }
            df = pd.DataFrame(sample_data)
            self.logger.info(f"Loaded {len(df)} property market records")
            return df
        except Exception as e:
            self.logger.error(f"Error loading property market data: {e}")
            return pd.DataFrame()

class LLMEconomicAnalyzer:
    """LLM-based economic analysis engine"""
    
    def __init__(self, data_loader: SilverDataLoader):
        self.data_loader = data_loader
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # In a real implementation, you would initialize your LLM client here
        # For example: OpenAI API, Anthropic Claude, or local models
        self.llm_available = False  # Set to True when LLM is configured
    
    def analyze_business_formation_trends(self) -> List[EconomicInsight]:
        """Analyze business formation trends using LLM"""
        insights = []
        
        try:
            acra_data = self.data_loader.load_acra_companies()
            
            if acra_data.empty:
                return insights
            
            # Calculate basic statistics
            total_companies = len(acra_data)
            active_companies = len(acra_data[acra_data['status'] == 'Active'])
            # Check for industry-related columns with fallback
            industry_column = None
            for col in ['industry_category', 'primary_ssic_description', 'industry_sector', 'business_sector']:
                if col in acra_data.columns:
                    industry_column = col
                    break
            
            tech_companies = 0
            if industry_column:
                tech_keywords = ['technology', 'software', 'digital', 'tech', 'IT', 'computer', 'information']
                tech_companies = len(acra_data[acra_data[industry_column].str.contains('|'.join(tech_keywords), case=False, na=False)])
            
            # Generate LLM-based insights
            if self.llm_available:
                # In real implementation, send data summary to LLM for analysis
                llm_analysis = self._query_llm_for_business_analysis(acra_data)
            else:
                # Fallback to rule-based analysis
                llm_analysis = self._generate_business_analysis_fallback(acra_data)
            
            insight = EconomicInsight(
                insight_type="business_formation",
                title="Business Formation Trend Analysis",
                description=llm_analysis,
                confidence_score=0.85,
                data_sources=["ACRA Companies"],
                timestamp=datetime.now(),
                metadata={
                    "total_companies": total_companies,
                    "active_companies": active_companies,
                    "tech_companies": tech_companies
                }
            )
            insights.append(insight)
            
        except Exception as e:
            self.logger.error(f"Error in business formation analysis: {e}")
        
        return insights
    
    def analyze_economic_indicators(self) -> List[EconomicInsight]:
        """Analyze economic indicators using LLM"""
        insights = []
        
        try:
            econ_data = self.data_loader.load_economic_indicators()
            
            if econ_data.empty:
                return insights
            
            # Generate LLM-based economic analysis
            if self.llm_available:
                llm_analysis = self._query_llm_for_economic_analysis(econ_data)
            else:
                llm_analysis = self._generate_economic_analysis_fallback(econ_data)
            
            insight = EconomicInsight(
                insight_type="economic_indicators",
                title="Economic Indicators Analysis",
                description=llm_analysis,
                confidence_score=0.90,
                data_sources=["SingStat Economic Indicators"],
                timestamp=datetime.now(),
                metadata=econ_data.to_dict('records')
            )
            insights.append(insight)
            
        except Exception as e:
            self.logger.error(f"Error in economic indicators analysis: {e}")
        
        return insights
    
    def analyze_government_impact(self) -> List[EconomicInsight]:
        """Analyze government expenditure impact using LLM"""
        insights = []
        
        try:
            gov_data = self.data_loader.load_government_expenditure()
            acra_data = self.data_loader.load_acra_companies()
            
            if gov_data.empty or acra_data.empty:
                return insights
            
            # Generate LLM-based government impact analysis
            if self.llm_available:
                llm_analysis = self._query_llm_for_government_analysis(gov_data, acra_data)
            else:
                llm_analysis = self._generate_government_analysis_fallback(gov_data, acra_data)
            
            insight = EconomicInsight(
                insight_type="government_impact",
                title="Government Expenditure Impact Analysis",
                description=llm_analysis,
                confidence_score=0.80,
                data_sources=["Government Expenditure", "ACRA Companies"],
                timestamp=datetime.now(),
                metadata={
                    "total_expenditure": gov_data['amount_million_sgd'].sum(),
                    "categories": gov_data['category'].tolist()
                }
            )
            insights.append(insight)
            
        except Exception as e:
            self.logger.error(f"Error in government impact analysis: {e}")
        
        return insights
    
    def _query_llm_for_business_analysis(self, data: pd.DataFrame) -> str:
        """Query LLM for business formation analysis"""
        # In real implementation, format data and send to LLM
        # Example prompt: "Analyze the following business formation data and provide insights..."
        return "LLM analysis would go here"
    
    def _query_llm_for_economic_analysis(self, data: pd.DataFrame) -> str:
        """Query LLM for economic indicators analysis"""
        return "LLM economic analysis would go here"
    
    def _query_llm_for_government_analysis(self, gov_data: pd.DataFrame, acra_data: pd.DataFrame) -> str:
        """Query LLM for government impact analysis"""
        return "LLM government impact analysis would go here"
    
    def _generate_business_analysis_fallback(self, data: pd.DataFrame) -> str:
        """Fallback analysis when LLM is not available"""
        total = len(data)
        active = len(data[data['status'] == 'Active'])
        # Check for industry-related columns with fallback
        industry_column = None
        for col in ['industry_category', 'primary_ssic_description', 'industry_sector', 'business_sector']:
            if col in data.columns:
                industry_column = col
                break
        
        tech_pct = 0
        if industry_column:
            tech_keywords = ['technology', 'software', 'digital', 'tech', 'IT', 'computer', 'information']
            tech_companies = len(data[data[industry_column].str.contains('|'.join(tech_keywords), case=False, na=False)])
            tech_pct = tech_companies / total * 100
        
        return f"""Business Formation Analysis:
        - Total companies analyzed: {total}
        - Active companies: {active} ({active/total*100:.1f}%)
        - Technology sector represents {tech_pct:.1f}% of new formations
        - Average data quality score: {data['data_quality_score'].mean():.2f}
        
        Key Insights:
        - Technology sector shows strong representation in new business formations
        - High data quality scores indicate reliable trend analysis
        - Business formation patterns suggest healthy economic activity"""
    
    def _generate_economic_analysis_fallback(self, data: pd.DataFrame) -> str:
        """Fallback economic analysis when LLM is not available"""
        gdp_growth = 0
        inflation = 0
        
        # Check for indicator_name column with fallback
        if 'indicator_name' in data.columns and not data.empty:
            gdp_data = data[data['indicator_name'] == 'GDP Growth Rate']
            if not gdp_data.empty and 'value' in gdp_data.columns:
                gdp_growth = gdp_data['value'].iloc[0]
            
            inflation_data = data[data['indicator_name'] == 'Inflation Rate']
            if not inflation_data.empty and 'value' in inflation_data.columns:
                inflation = inflation_data['value'].iloc[0]
        
        return f"""Economic Indicators Analysis:
        - GDP Growth Rate: {gdp_growth}% (Q4 2023)
        - Inflation Rate: {inflation}% (Q4 2023)
        
        Key Insights:
        - GDP growth indicates positive economic momentum
        - Inflation levels suggest controlled price pressures
        - Economic fundamentals support business investment climate"""
    
    def _generate_government_analysis_fallback(self, gov_data: pd.DataFrame, acra_data: pd.DataFrame) -> str:
        """Fallback government impact analysis when LLM is not available"""
        total_spending = gov_data['amount_million_sgd'].sum()
        infrastructure_spending = gov_data[gov_data['category'] == 'Infrastructure']['amount_million_sgd'].sum()
        
        return f"""Government Expenditure Impact Analysis:
        - Total government expenditure: S${total_spending:.0f} million
        - Infrastructure investment: S${infrastructure_spending:.0f} million
        
        Key Insights:
        - Significant infrastructure investment likely supports business formation
        - Government spending patterns indicate pro-business policy environment
        - Public investment creates opportunities for private sector growth"""

class AnomalyDetector:
    """LLM-based anomaly detection for economic data"""
    
    def __init__(self, data_loader: SilverDataLoader):
        self.data_loader = data_loader
        self.logger = logging.getLogger(self.__class__.__name__)
        self.llm_available = False
    
    def detect_business_formation_anomalies(self) -> List[AnomalyAlert]:
        """Detect anomalies in business formation patterns"""
        alerts = []
        
        try:
            acra_data = self.data_loader.load_acra_companies()
            
            if acra_data.empty:
                return alerts
            
            # Simple rule-based anomaly detection (would be enhanced with LLM)
            tech_ratio = 0
            # Check for industry-related columns with fallback
            industry_column = None
            for col in ['industry_category', 'primary_ssic_description', 'industry_sector', 'business_sector']:
                if col in acra_data.columns:
                    industry_column = col
                    break
            
            if industry_column:
                tech_companies = acra_data[acra_data[industry_column].str.contains('Technology|tech|IT|software', case=False, na=False)]
                tech_ratio = len(tech_companies) / len(acra_data)
            
            if tech_ratio > 0.5:  # More than 50% tech companies might be unusual
                alert = AnomalyAlert(
                    alert_type="business_formation",
                    severity="medium",
                    description=f"Unusually high technology sector concentration: {tech_ratio:.1%}",
                    affected_metrics=["industry_distribution"],
                    timestamp=datetime.now(),
                    confidence_score=0.75,
                    recommended_actions=[
                        "Investigate technology sector trends",
                        "Analyze market conditions driving tech formations",
                        "Monitor for potential bubble indicators"
                    ]
                )
                alerts.append(alert)
            
        except Exception as e:
            self.logger.error(f"Error in anomaly detection: {e}")
        
        return alerts
    
    def detect_economic_anomalies(self) -> List[AnomalyAlert]:
        """Detect anomalies in economic indicators"""
        alerts = []
        
        try:
            econ_data = self.data_loader.load_economic_indicators()
            
            if econ_data.empty:
                return alerts
            
            # Check for unusual economic indicator values
            gdp_growth = econ_data[econ_data['indicator_name'] == 'GDP Growth Rate']['value']
            if not gdp_growth.empty and gdp_growth.iloc[0] < 0:
                alert = AnomalyAlert(
                    alert_type="economic_indicators",
                    severity="high",
                    description=f"Negative GDP growth detected: {gdp_growth.iloc[0]}%",
                    affected_metrics=["gdp_growth"],
                    timestamp=datetime.now(),
                    confidence_score=0.95,
                    recommended_actions=[
                        "Investigate economic downturn causes",
                        "Monitor business formation impacts",
                        "Assess policy intervention needs"
                    ]
                )
                alerts.append(alert)
            
        except Exception as e:
            self.logger.error(f"Error in economic anomaly detection: {e}")
        
        return alerts

class EconomicForecaster:
    """LLM-based economic forecasting"""
    
    def __init__(self, data_loader: SilverDataLoader):
        self.data_loader = data_loader
        self.logger = logging.getLogger(self.__class__.__name__)
        self.llm_available = False
    
    def forecast_business_trends(self, horizon_months: int = 6) -> Dict[str, Any]:
        """Forecast business formation trends"""
        try:
            acra_data = self.data_loader.load_acra_companies()
            econ_data = self.data_loader.load_economic_indicators()
            
            if acra_data.empty or econ_data.empty:
                return {}
            
            # Simple trend-based forecasting (would be enhanced with LLM)
            current_formation_rate = len(acra_data) / 3  # Assuming 3 months of data
            
            forecast = {
                "forecast_horizon_months": horizon_months,
                "predicted_formations_per_month": current_formation_rate * 1.05,  # 5% growth assumption
                "confidence_interval": [current_formation_rate * 0.9, current_formation_rate * 1.2],
                "key_factors": [
                    "Economic growth momentum",
                    "Government policy support",
                    "Technology sector expansion"
                ],
                "forecast_timestamp": datetime.now().isoformat()
            }
            
            return forecast
            
        except Exception as e:
            self.logger.error(f"Error in business trend forecasting: {e}")
            return {}
    
    def forecast_economic_indicators(self, horizon_quarters: int = 2) -> Dict[str, Any]:
        """Forecast economic indicators"""
        try:
            econ_data = self.data_loader.load_economic_indicators()
            
            if econ_data.empty:
                return {}
            
            # Simple forecasting based on current values
            gdp_current = econ_data[econ_data['indicator_name'] == 'GDP Growth Rate']['value'].iloc[0]
            inflation_current = econ_data[econ_data['indicator_name'] == 'Inflation Rate']['value'].iloc[0]
            
            forecast = {
                "forecast_horizon_quarters": horizon_quarters,
                "gdp_growth_forecast": gdp_current * 0.95,  # Slight moderation
                "inflation_forecast": inflation_current * 1.02,  # Slight increase
                "forecast_confidence": 0.70,
                "forecast_timestamp": datetime.now().isoformat()
            }
            
            return forecast
            
        except Exception as e:
            self.logger.error(f"Error in economic indicator forecasting: {e}")
            return {}

class EconomicIntelligencePlatform:
    """Main platform orchestrating all LLM-based analytics"""
    
    def __init__(self, data_path: str = "/tmp/silver_data"):
        self.data_loader = SilverDataLoader(data_path)
        self.analyzer = LLMEconomicAnalyzer(self.data_loader)
        self.anomaly_detector = AnomalyDetector(self.data_loader)
        self.forecaster = EconomicForecaster(self.data_loader)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """Generate comprehensive economic intelligence report"""
        self.logger.info("Generating comprehensive economic intelligence report...")
        
        report = {
            "report_timestamp": datetime.now().isoformat(),
            "report_type": "comprehensive_economic_intelligence",
            "insights": [],
            "anomalies": [],
            "forecasts": {},
            "data_quality": {},
            "recommendations": []
        }
        
        try:
            # Generate insights
            business_insights = self.analyzer.analyze_business_formation_trends()
            economic_insights = self.analyzer.analyze_economic_indicators()
            government_insights = self.analyzer.analyze_government_impact()
            
            report["insights"] = [
                self._insight_to_dict(insight) 
                for insight in business_insights + economic_insights + government_insights
            ]
            
            # Detect anomalies
            business_anomalies = self.anomaly_detector.detect_business_formation_anomalies()
            economic_anomalies = self.anomaly_detector.detect_economic_anomalies()
            
            report["anomalies"] = [
                self._anomaly_to_dict(anomaly)
                for anomaly in business_anomalies + economic_anomalies
            ]
            
            # Generate forecasts
            business_forecast = self.forecaster.forecast_business_trends()
            economic_forecast = self.forecaster.forecast_economic_indicators()
            
            report["forecasts"] = {
                "business_trends": business_forecast,
                "economic_indicators": economic_forecast
            }
            
            # Add recommendations
            report["recommendations"] = self._generate_recommendations(report)
            
            self.logger.info("Comprehensive report generated successfully")
            
        except Exception as e:
            self.logger.error(f"Error generating comprehensive report: {e}")
            report["error"] = str(e)
        
        return report
    
    def _insight_to_dict(self, insight: EconomicInsight) -> Dict[str, Any]:
        """Convert EconomicInsight to dictionary"""
        return {
            "insight_type": insight.insight_type,
            "title": insight.title,
            "description": insight.description,
            "confidence_score": insight.confidence_score,
            "data_sources": insight.data_sources,
            "timestamp": insight.timestamp.isoformat(),
            "metadata": insight.metadata
        }
    
    def _anomaly_to_dict(self, anomaly: AnomalyAlert) -> Dict[str, Any]:
        """Convert AnomalyAlert to dictionary"""
        return {
            "alert_type": anomaly.alert_type,
            "severity": anomaly.severity,
            "description": anomaly.description,
            "affected_metrics": anomaly.affected_metrics,
            "timestamp": anomaly.timestamp.isoformat(),
            "confidence_score": anomaly.confidence_score,
            "recommended_actions": anomaly.recommended_actions
        }
    
    def _generate_recommendations(self, report: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on analysis"""
        recommendations = []
        
        # Based on insights
        if report["insights"]:
            recommendations.append("Monitor business formation trends for policy impact assessment")
            recommendations.append("Continue tracking economic indicators for early warning signals")
        
        # Based on anomalies
        if report["anomalies"]:
            high_severity_anomalies = [a for a in report["anomalies"] if a["severity"] in ["high", "critical"]]
            if high_severity_anomalies:
                recommendations.append("Immediate investigation required for high-severity anomalies")
        
        # Based on forecasts
        if report["forecasts"]:
            recommendations.append("Use forecasts for strategic planning and resource allocation")
            recommendations.append("Validate forecast assumptions with domain experts")
        
        return recommendations
    
    def save_report(self, report: Dict[str, Any], output_path: str = None) -> str:
        """Save report to file"""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"/tmp/economic_intelligence_report_{timestamp}.json"
        
        try:
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            self.logger.info(f"Report saved to {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Error saving report: {e}")
            raise

def main():
    """Main function for testing the economic intelligence platform"""
    print("🧠 LLM-Based Economic Intelligence Platform")
    print("=" * 50)
    
    # Initialize platform
    platform = EconomicIntelligencePlatform()
    
    # Generate comprehensive report
    print("\n📊 Generating comprehensive economic intelligence report...")
    report = platform.generate_comprehensive_report()
    
    # Display summary
    print(f"\n✅ Report generated with:")
    print(f"   - {len(report.get('insights', []))} economic insights")
    print(f"   - {len(report.get('anomalies', []))} anomaly alerts")
    print(f"   - {len(report.get('forecasts', {}))} forecast categories")
    print(f"   - {len(report.get('recommendations', []))} recommendations")
    
    # Save report
    output_path = platform.save_report(report)
    print(f"\n💾 Report saved to: {output_path}")
    
    # Display sample insights
    if report.get('insights'):
        print("\n🔍 Sample Insights:")
        for insight in report['insights'][:2]:  # Show first 2 insights
            print(f"   • {insight['title']} (Confidence: {insight['confidence_score']:.0%})")
    
    if report.get('anomalies'):
        print("\n⚠️  Anomaly Alerts:")
        for anomaly in report['anomalies']:
            print(f"   • {anomaly['severity'].upper()}: {anomaly['description']}")
    
    print("\n🎯 Next Steps:")
    print("   1. Configure LLM integration for enhanced analysis")
    print("   2. Connect to actual Silver layer data sources")
    print("   3. Set up automated report generation")
    print("   4. Integrate with visualization dashboard")

if __name__ == "__main__":
    main()