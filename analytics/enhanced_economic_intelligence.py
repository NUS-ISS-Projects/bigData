#!/usr/bin/env python3
"""
Enhanced LLM-Based Economic Intelligence Platform
Integrates with actual Silver layer data and LLM providers
Implements the complete Analytics & ML layer (Section D)
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import pandas as pd
import numpy as np
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

# Import our custom modules
from analytics.silver_data_connector import SilverLayerConnector, DataSourceConfig
from analytics.llm_config import (
    LLMClient, LLMConfig, LLMProvider, EconomicAnalysisPrompts,
    create_llm_client, get_default_config
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class EnhancedEconomicInsight:
    """Enhanced economic insight with LLM analysis"""
    insight_type: str
    title: str
    description: str
    llm_analysis: str
    confidence_score: float
    data_sources: List[str]
    timestamp: datetime
    metadata: Dict[str, Any]
    key_metrics: Dict[str, float]
    recommendations: List[str]
    risk_factors: List[str]

@dataclass
class EnhancedAnomalyAlert:
    """Enhanced anomaly alert with LLM explanation"""
    alert_type: str
    severity: str
    description: str
    llm_explanation: str
    affected_metrics: List[str]
    timestamp: datetime
    confidence_score: float
    recommended_actions: List[str]
    potential_causes: List[str]
    monitoring_requirements: List[str]

@dataclass
class EconomicForecast:
    """Economic forecast with LLM insights"""
    forecast_type: str
    horizon_months: int
    predictions: Dict[str, Any]
    confidence_intervals: Dict[str, Tuple[float, float]]
    key_assumptions: List[str]
    scenario_analysis: Dict[str, Dict[str, Any]]
    llm_commentary: str
    risk_assessment: str
    timestamp: datetime

class EnhancedLLMEconomicAnalyzer:
    """Enhanced economic analyzer with real data and LLM integration"""
    
    def __init__(self, data_connector: SilverLayerConnector, llm_client: LLMClient = None):
        self.data_connector = data_connector
        self.llm_client = llm_client
        self.logger = logging.getLogger(self.__class__.__name__)
        self.prompts = EconomicAnalysisPrompts()
        
        # Initialize LLM client if not provided
        if self.llm_client is None:
            try:
                config = get_default_config()
                self.llm_client = create_llm_client(config)
            except Exception as e:
                self.logger.warning(f"Could not initialize LLM client: {e}")
                self.llm_client = None
    
    def analyze_business_formation_trends(self) -> List[EnhancedEconomicInsight]:
        """Analyze business formation trends with LLM enhancement"""
        insights = []
        
        try:
            # Load real data from silver layer
            acra_data = self.data_connector.load_acra_companies()
            
            if acra_data.empty:
                self.logger.warning("No ACRA data available")
                return insights
            
            # Calculate key metrics
            key_metrics = self._calculate_business_metrics(acra_data)
            
            # Generate LLM analysis if available
            llm_analysis = ""
            if self.llm_client and self.llm_client.is_available():
                try:
                    context_data = {
                        "data_summary": key_metrics,
                        "sample_records": acra_data.head(10).to_dict('records'),
                        "industry_distribution": acra_data['industry_category'].value_counts().to_dict()
                    }
                    
                    llm_analysis = self.llm_client.generate_analysis(
                        self.prompts.business_formation_analysis(),
                        context_data
                    )
                except Exception as e:
                    self.logger.error(f"LLM analysis failed: {e}")
                    llm_analysis = "LLM analysis unavailable"
            else:
                llm_analysis = self._generate_business_analysis_fallback(acra_data, key_metrics)
            
            # Extract recommendations and risk factors from analysis
            recommendations, risk_factors = self._extract_insights_from_analysis(llm_analysis)
            
            insight = EnhancedEconomicInsight(
                insight_type="business_formation",
                title="Business Formation Trend Analysis",
                description=f"Analysis of {len(acra_data)} business registrations across {acra_data['industry_category'].nunique()} industries",
                llm_analysis=llm_analysis,
                confidence_score=0.85 if self.llm_client and self.llm_client.is_available() else 0.70,
                data_sources=["ACRA Companies (Silver Layer)"],
                timestamp=datetime.now(),
                metadata={
                    "data_period": {
                        "start": acra_data['registration_date'].min().isoformat() if 'registration_date' in acra_data.columns else None,
                        "end": acra_data['registration_date'].max().isoformat() if 'registration_date' in acra_data.columns else None
                    },
                    "data_quality": acra_data['data_quality_score'].mean() if 'data_quality_score' in acra_data.columns else None
                },
                key_metrics=key_metrics,
                recommendations=recommendations,
                risk_factors=risk_factors
            )
            insights.append(insight)
            
        except Exception as e:
            self.logger.error(f"Error in business formation analysis: {e}")
        
        return insights
    
    def analyze_economic_indicators(self) -> List[EnhancedEconomicInsight]:
        """Analyze economic indicators with LLM enhancement"""
        insights = []
        
        try:
            # Load real data from silver layer
            econ_data = self.data_connector.load_economic_indicators()
            
            if econ_data.empty:
                self.logger.warning("No economic indicators data available")
                return insights
            
            # Calculate key metrics
            key_metrics = self._calculate_economic_metrics(econ_data)
            
            # Generate LLM analysis
            llm_analysis = ""
            if self.llm_client and self.llm_client.is_available():
                try:
                    context_data = {
                        "indicators_summary": key_metrics,
                        "latest_values": econ_data.groupby('indicator_name')['value'].last().to_dict(),
                        "data_coverage": {
                            "indicators_count": econ_data['indicator_name'].nunique(),
                            "latest_period": econ_data['period'].max() if 'period' in econ_data.columns else None
                        }
                    }
                    
                    llm_analysis = self.llm_client.generate_analysis(
                        self.prompts.economic_indicators_analysis(),
                        context_data
                    )
                except Exception as e:
                    self.logger.error(f"LLM analysis failed: {e}")
                    llm_analysis = "LLM analysis unavailable"
            else:
                llm_analysis = self._generate_economic_analysis_fallback(econ_data, key_metrics)
            
            recommendations, risk_factors = self._extract_insights_from_analysis(llm_analysis)
            
            insight = EnhancedEconomicInsight(
                insight_type="economic_indicators",
                title="Economic Indicators Analysis",
                description=f"Analysis of {econ_data['indicator_name'].nunique()} economic indicators",
                llm_analysis=llm_analysis,
                confidence_score=0.90 if self.llm_client and self.llm_client.is_available() else 0.75,
                data_sources=["SingStat Economic Indicators (Silver Layer)"],
                timestamp=datetime.now(),
                metadata={
                    "indicators_analyzed": econ_data['indicator_name'].unique().tolist(),
                    "data_quality": econ_data['data_quality_score'].mean() if 'data_quality_score' in econ_data.columns else None
                },
                key_metrics=key_metrics,
                recommendations=recommendations,
                risk_factors=risk_factors
            )
            insights.append(insight)
            
        except Exception as e:
            self.logger.error(f"Error in economic indicators analysis: {e}")
        
        return insights
    
    def analyze_cross_sector_correlations(self) -> List[EnhancedEconomicInsight]:
        """Analyze correlations across different data sources"""
        insights = []
        
        try:
            # Load data from multiple sources
            acra_data = self.data_connector.load_acra_companies()
            econ_data = self.data_connector.load_economic_indicators()
            gov_data = self.data_connector.load_government_expenditure()
            prop_data = self.data_connector.load_property_market()
            
            # Calculate cross-sector metrics
            key_metrics = self._calculate_cross_sector_metrics(
                acra_data, econ_data, gov_data, prop_data
            )
            
            # Generate LLM analysis for cross-sector insights
            llm_analysis = ""
            if self.llm_client and self.llm_client.is_available():
                try:
                    context_data = {
                        "cross_sector_metrics": key_metrics,
                        "data_availability": {
                            "acra_records": len(acra_data),
                            "economic_indicators": len(econ_data),
                            "government_expenditure": len(gov_data),
                            "property_market": len(prop_data)
                        }
                    }
                    
                    cross_sector_prompt = """
                    Analyze the cross-sector correlations and relationships in Singapore's economy based on the provided data. Focus on:
                    
                    1. Business formation patterns vs economic indicators
                    2. Government expenditure impact on business activity
                    3. Property market trends and business formation
                    4. Sector-specific growth patterns
                    5. Policy effectiveness indicators
                    
                    Provide insights on:
                    - Key economic relationships and dependencies
                    - Policy intervention opportunities
                    - Risk factors from sector imbalances
                    - Strategic recommendations for economic development
                    """
                    
                    llm_analysis = self.llm_client.generate_analysis(
                        cross_sector_prompt,
                        context_data
                    )
                except Exception as e:
                    self.logger.error(f"Cross-sector LLM analysis failed: {e}")
                    llm_analysis = "Cross-sector LLM analysis unavailable"
            else:
                llm_analysis = self._generate_cross_sector_analysis_fallback(key_metrics)
            
            recommendations, risk_factors = self._extract_insights_from_analysis(llm_analysis)
            
            insight = EnhancedEconomicInsight(
                insight_type="cross_sector_analysis",
                title="Cross-Sector Economic Correlation Analysis",
                description="Comprehensive analysis of relationships between business formation, economic indicators, government spending, and property markets",
                llm_analysis=llm_analysis,
                confidence_score=0.80 if self.llm_client and self.llm_client.is_available() else 0.65,
                data_sources=[
                    "ACRA Companies (Silver Layer)",
                    "SingStat Economic Indicators (Silver Layer)",
                    "Government Expenditure (Silver Layer)",
                    "URA Property Market (Silver Layer)"
                ],
                timestamp=datetime.now(),
                metadata={
                    "analysis_scope": "multi_sector_correlation",
                    "data_sources_count": 4
                },
                key_metrics=key_metrics,
                recommendations=recommendations,
                risk_factors=risk_factors
            )
            insights.append(insight)
            
        except Exception as e:
            self.logger.error(f"Error in cross-sector analysis: {e}")
        
        return insights
    
    def _calculate_business_metrics(self, acra_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate key business formation metrics"""
        metrics = {}
        
        try:
            metrics['total_companies'] = len(acra_data)
            
            if 'entity_status' in acra_data.columns:
                metrics['active_companies'] = len(acra_data[acra_data['entity_status'] == 'REGISTERED'])
                metrics['active_rate'] = metrics['active_companies'] / metrics['total_companies']
            
            if 'industry_category' in acra_data.columns:
                metrics['industry_diversity'] = acra_data['industry_category'].nunique()
                top_industry = acra_data['industry_category'].value_counts().index[0]
                metrics['top_industry_concentration'] = (
                    acra_data['industry_category'].value_counts().iloc[0] / len(acra_data)
                )
            
            if 'data_quality_score' in acra_data.columns:
                metrics['avg_data_quality'] = acra_data['data_quality_score'].mean()
            
            if 'registration_date' in acra_data.columns:
                acra_data['registration_date'] = pd.to_datetime(acra_data['registration_date'])
                recent_registrations = acra_data[
                    acra_data['registration_date'] >= (datetime.now() - timedelta(days=90))
                ]
                metrics['recent_formation_rate'] = len(recent_registrations) / 90  # per day
            
        except Exception as e:
            self.logger.error(f"Error calculating business metrics: {e}")
        
        return metrics
    
    def _calculate_economic_metrics(self, econ_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate key economic metrics"""
        metrics = {}
        
        try:
            metrics['indicators_count'] = econ_data['indicator_name'].nunique() if 'indicator_name' in econ_data.columns else 0
            
            if 'data_quality_score' in econ_data.columns:
                metrics['avg_data_quality'] = econ_data['data_quality_score'].mean()
            
            # Get latest values for key indicators
            if 'indicator_name' in econ_data.columns and 'value' in econ_data.columns:
                latest_values = econ_data.groupby('indicator_name')['value'].last()
                
                for indicator in ['GDP Growth Rate', 'Inflation Rate', 'Unemployment Rate']:
                    if indicator in latest_values.index:
                        metrics[f'latest_{indicator.lower().replace(" ", "_").replace("%", "_rate")}'] = latest_values[indicator]
            
            # Calculate volatility if we have time series data
            if 'period' in econ_data.columns and 'value' in econ_data.columns:
                for indicator in econ_data['indicator_name'].unique():
                    indicator_data = econ_data[econ_data['indicator_name'] == indicator]['value']
                    if len(indicator_data) > 1:
                        volatility = indicator_data.std()
                        metrics[f'{indicator.lower().replace(" ", "_")}_volatility'] = volatility
            
        except Exception as e:
            self.logger.error(f"Error calculating economic metrics: {e}")
        
        return metrics
    
    def _calculate_cross_sector_metrics(self, acra_data: pd.DataFrame, econ_data: pd.DataFrame, 
                                      gov_data: pd.DataFrame, prop_data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate cross-sector correlation metrics"""
        metrics = {}
        
        try:
            # Data availability metrics
            metrics['data_coverage'] = {
                'acra_companies': len(acra_data),
                'economic_indicators': len(econ_data),
                'government_expenditure': len(gov_data),
                'property_market': len(prop_data)
            }
            
            # Business formation vs economic health
            if not acra_data.empty and not econ_data.empty:
                if 'status' in acra_data.columns:
                    active_rate = len(acra_data[acra_data['status'] == 'Active']) / len(acra_data)
                    metrics['business_health_score'] = active_rate
            
            # Government spending efficiency
            if not gov_data.empty and not acra_data.empty:
                if 'amount_million_sgd' in gov_data.columns:
                    total_gov_spending = gov_data['amount_million_sgd'].sum()
                    business_formation_rate = len(acra_data) / 365  # assuming annual data
                    metrics['spending_per_business_formation'] = total_gov_spending / max(business_formation_rate, 1)
            
            # Property market vs business activity
            if not prop_data.empty and not acra_data.empty:
                if 'median_rental_psf' in prop_data.columns:
                    avg_rental = prop_data['median_rental_psf'].mean()
                    tech_companies = len(acra_data[acra_data['industry_category'] == 'Technology']) if 'industry_category' in acra_data.columns else 0
                    metrics['rental_vs_tech_formation'] = tech_companies / max(avg_rental, 1)
            
            # Sector diversity index
            if 'industry_category' in acra_data.columns:
                industry_counts = acra_data['industry_category'].value_counts()
                # Calculate Herfindahl-Hirschman Index for industry concentration
                total_companies = len(acra_data)
                hhi = sum((count / total_companies) ** 2 for count in industry_counts)
                metrics['industry_concentration_index'] = hhi
                metrics['industry_diversity_score'] = 1 - hhi  # Higher score = more diverse
            
        except Exception as e:
            self.logger.error(f"Error calculating cross-sector metrics: {e}")
        
        return metrics
    
    def _extract_insights_from_analysis(self, analysis_text: str) -> Tuple[List[str], List[str]]:
        """Extract recommendations and risk factors from LLM analysis"""
        recommendations = []
        risk_factors = []
        
        try:
            # Simple keyword-based extraction (could be enhanced with NLP)
            lines = analysis_text.split('\n')
            
            in_recommendations = False
            in_risks = False
            
            for line in lines:
                line = line.strip()
                
                # Check for section headers
                if any(keyword in line.lower() for keyword in ['recommendation', 'suggest', 'should', 'action']):
                    in_recommendations = True
                    in_risks = False
                elif any(keyword in line.lower() for keyword in ['risk', 'concern', 'warning', 'threat']):
                    in_risks = True
                    in_recommendations = False
                
                # Extract bullet points or numbered items
                if line.startswith(('-', '•', '*')) or (len(line) > 0 and line[0].isdigit()):
                    clean_line = line.lstrip('-•*0123456789. ').strip()
                    if clean_line:
                        if in_recommendations:
                            recommendations.append(clean_line)
                        elif in_risks:
                            risk_factors.append(clean_line)
            
            # Fallback: extract from common patterns
            if not recommendations:
                for line in lines:
                    if any(keyword in line.lower() for keyword in ['recommend', 'suggest', 'should']):
                        recommendations.append(line.strip())
            
            if not risk_factors:
                for line in lines:
                    if any(keyword in line.lower() for keyword in ['risk', 'concern', 'warning']):
                        risk_factors.append(line.strip())
        
        except Exception as e:
            self.logger.error(f"Error extracting insights: {e}")
        
        return recommendations[:5], risk_factors[:5]  # Limit to top 5 each
    
    def _generate_business_analysis_fallback(self, data: pd.DataFrame, metrics: Dict[str, float]) -> str:
        """Fallback business analysis when LLM is not available"""
        total = metrics.get('total_companies', 0)
        active_rate = metrics.get('active_rate', 0) * 100
        diversity = metrics.get('industry_diversity', 0)
        quality = metrics.get('avg_data_quality', 0)
        
        return f"""Business Formation Analysis (Rule-based):
        
Key Findings:
        - Total companies analyzed: {total:,.0f}
        - Active company rate: {active_rate:.1f}%
        - Industry diversity: {diversity} distinct sectors
        - Average data quality: {quality:.2f}
        
Insights:
        - Business formation shows {'healthy' if active_rate > 80 else 'moderate'} activity levels
        - Industry diversification is {'strong' if diversity > 10 else 'developing'}
        - Data quality is {'excellent' if quality > 0.9 else 'good' if quality > 0.8 else 'acceptable'}
        
Recommendations:
        - Monitor sector concentration for balanced growth
        - Support emerging industries for economic resilience
        - Maintain data quality standards for reliable analysis
        
Risk Factors:
        - {'High sector concentration risk' if metrics.get('top_industry_concentration', 0) > 0.4 else 'Balanced sector distribution'}
        - {'Data quality concerns' if quality < 0.8 else 'Data quality acceptable'}
        """
    
    def _generate_economic_analysis_fallback(self, data: pd.DataFrame, metrics: Dict[str, float]) -> str:
        """Fallback economic analysis when LLM is not available"""
        indicators_count = metrics.get('indicators_count', 0)
        quality = metrics.get('avg_data_quality', 0)
        
        gdp_growth = metrics.get('latest_gdp_growth_rate', 0)
        inflation = metrics.get('latest_inflation_rate', 0)
        unemployment = metrics.get('latest_unemployment_rate', 0)
        
        return f"""Economic Indicators Analysis (Rule-based):
        
Key Metrics:
        - Economic indicators tracked: {indicators_count}
        - GDP Growth Rate: {gdp_growth:.1f}%
        - Inflation Rate: {inflation:.1f}%
        - Unemployment Rate: {unemployment:.1f}%
        - Data quality: {quality:.2f}
        
Economic Assessment:
        - GDP growth indicates {'strong' if gdp_growth > 3 else 'moderate' if gdp_growth > 1 else 'weak'} economic momentum
        - Inflation is {'concerning' if inflation > 4 else 'elevated' if inflation > 2.5 else 'controlled'}
        - Employment situation is {'healthy' if unemployment < 3 else 'stable' if unemployment < 5 else 'concerning'}
        
Recommendations:
        - Continue monitoring inflation trends for policy adjustments
        - Support employment programs if unemployment rises
        - Maintain economic diversification strategies
        
Risk Factors:
        - {'Inflation pressure risk' if inflation > 3 else 'Inflation under control'}
        - {'Economic slowdown risk' if gdp_growth < 1 else 'Economic growth sustainable'}
        """
    
    def _generate_cross_sector_analysis_fallback(self, metrics: Dict[str, Any]) -> str:
        """Fallback cross-sector analysis when LLM is not available"""
        data_coverage = metrics.get('data_coverage', {})
        diversity_score = metrics.get('industry_diversity_score', 0)
        
        return f"""Cross-Sector Economic Analysis (Rule-based):
        
Data Coverage:
        - ACRA Companies: {data_coverage.get('acra_companies', 0):,} records
        - Economic Indicators: {data_coverage.get('economic_indicators', 0):,} records
        - Government Expenditure: {data_coverage.get('government_expenditure', 0):,} records
        - Property Market: {data_coverage.get('property_market', 0):,} records
        
Cross-Sector Insights:
        - Industry diversity score: {diversity_score:.2f}
        - Economic integration: {'High' if diversity_score > 0.8 else 'Moderate' if diversity_score > 0.6 else 'Low'}
        - Data completeness: {'Comprehensive' if sum(data_coverage.values()) > 1000 else 'Adequate'}
        
Recommendations:
        - Enhance data integration across sectors
        - Monitor cross-sector dependencies
        - Develop sector-specific policy interventions
        
Risk Factors:
        - Sector concentration risks
        - Data quality variations across sources
        - Policy coordination challenges
        """

class EnhancedAnomalyDetector:
    """Enhanced anomaly detection with LLM explanations"""
    
    def __init__(self, data_connector: SilverLayerConnector, llm_client: LLMClient = None):
        self.data_connector = data_connector
        self.llm_client = llm_client
        self.logger = logging.getLogger(self.__class__.__name__)
        self.prompts = EconomicAnalysisPrompts()
    
    def detect_comprehensive_anomalies(self) -> List[EnhancedAnomalyAlert]:
        """Detect anomalies across all data sources with LLM explanations"""
        alerts = []
        
        try:
            # Load data from all sources
            acra_data = self.data_connector.load_acra_companies()
            econ_data = self.data_connector.load_economic_indicators()
            gov_data = self.data_connector.load_government_expenditure()
            prop_data = self.data_connector.load_property_market()
            
            # Detect anomalies in each dataset
            business_anomalies = self._detect_business_anomalies(acra_data)
            economic_anomalies = self._detect_economic_anomalies(econ_data)
            government_anomalies = self._detect_government_anomalies(gov_data)
            property_anomalies = self._detect_property_anomalies(prop_data)
            
            # Combine all anomalies
            all_anomalies = business_anomalies + economic_anomalies + government_anomalies + property_anomalies
            
            # Enhance with LLM explanations
            for anomaly in all_anomalies:
                enhanced_anomaly = self._enhance_anomaly_with_llm(anomaly)
                alerts.append(enhanced_anomaly)
            
        except Exception as e:
            self.logger.error(f"Error in comprehensive anomaly detection: {e}")
        
        return alerts
    
    def _detect_business_anomalies(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Detect anomalies in business formation data"""
        anomalies = []
        
        try:
            if data.empty:
                return anomalies
            
            # Industry concentration anomaly
            if 'industry_category' in data.columns:
                industry_counts = data['industry_category'].value_counts()
                top_industry_pct = industry_counts.iloc[0] / len(data)
                
                if top_industry_pct > 0.5:
                    anomalies.append({
                        'type': 'business_formation',
                        'severity': 'medium',
                        'description': f'High industry concentration: {industry_counts.index[0]} represents {top_industry_pct:.1%} of formations',
                        'affected_metrics': ['industry_distribution'],
                        'data_context': {
                            'top_industry': industry_counts.index[0],
                            'concentration_pct': top_industry_pct,
                            'total_industries': len(industry_counts)
                        }
                    })
            
            # Data quality anomaly
            if 'data_quality_score' in data.columns:
                low_quality_pct = len(data[data['data_quality_score'] < 0.7]) / len(data)
                
                if low_quality_pct > 0.2:
                    anomalies.append({
                        'type': 'data_quality',
                        'severity': 'high',
                        'description': f'High proportion of low-quality records: {low_quality_pct:.1%}',
                        'affected_metrics': ['data_quality_score'],
                        'data_context': {
                            'low_quality_pct': low_quality_pct,
                            'avg_quality': data['data_quality_score'].mean()
                        }
                    })
            
        except Exception as e:
            self.logger.error(f"Error detecting business anomalies: {e}")
        
        return anomalies
    
    def _detect_economic_anomalies(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Detect anomalies in economic indicators"""
        anomalies = []
        
        try:
            if data.empty:
                return anomalies
            
            # Check for extreme values in key indicators
            if 'indicator_name' in data.columns and 'value' in data.columns:
                latest_values = data.groupby('indicator_name')['value'].last()
                
                # GDP growth anomaly
                if 'GDP Growth Rate' in latest_values.index:
                    gdp_growth = latest_values['GDP Growth Rate']
                    if gdp_growth < -2:
                        anomalies.append({
                            'type': 'economic_indicators',
                            'severity': 'critical',
                            'description': f'Severe economic contraction: GDP growth at {gdp_growth:.1f}%',
                            'affected_metrics': ['gdp_growth'],
                            'data_context': {'gdp_growth': gdp_growth}
                        })
                    elif gdp_growth > 8:
                        anomalies.append({
                            'type': 'economic_indicators',
                            'severity': 'medium',
                            'description': f'Unusually high GDP growth: {gdp_growth:.1f}%',
                            'affected_metrics': ['gdp_growth'],
                            'data_context': {'gdp_growth': gdp_growth}
                        })
                
                # Inflation anomaly
                if 'Inflation Rate' in latest_values.index:
                    inflation = latest_values['Inflation Rate']
                    if inflation > 5:
                        anomalies.append({
                            'type': 'economic_indicators',
                            'severity': 'high',
                            'description': f'High inflation detected: {inflation:.1f}%',
                            'affected_metrics': ['inflation_rate'],
                            'data_context': {'inflation_rate': inflation}
                        })
        
        except Exception as e:
            self.logger.error(f"Error detecting economic anomalies: {e}")
        
        return anomalies
    
    def _detect_government_anomalies(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Detect anomalies in government expenditure"""
        anomalies = []
        
        try:
            if data.empty:
                return anomalies
            
            # Unusual spending patterns
            if 'amount_million_sgd' in data.columns and 'category' in data.columns:
                category_spending = data.groupby('category')['amount_million_sgd'].sum()
                total_spending = category_spending.sum()
                
                # Check for extreme concentration in one category
                max_category_pct = category_spending.max() / total_spending
                if max_category_pct > 0.6:
                    max_category = category_spending.idxmax()
                    anomalies.append({
                        'type': 'government_expenditure',
                        'severity': 'medium',
                        'description': f'High expenditure concentration in {max_category}: {max_category_pct:.1%}',
                        'affected_metrics': ['expenditure_distribution'],
                        'data_context': {
                            'dominant_category': max_category,
                            'concentration_pct': max_category_pct
                        }
                    })
        
        except Exception as e:
            self.logger.error(f"Error detecting government anomalies: {e}")
        
        return anomalies
    
    def _detect_property_anomalies(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Detect anomalies in property market data"""
        anomalies = []
        
        try:
            if data.empty:
                return anomalies
            
            # Extreme rental prices
            if 'median_rental_psf' in data.columns:
                rental_stats = data['median_rental_psf'].describe()
                q75 = rental_stats['75%']
                q25 = rental_stats['25%']
                iqr = q75 - q25
                upper_bound = q75 + 1.5 * iqr
                
                extreme_rentals = data[data['median_rental_psf'] > upper_bound]
                if len(extreme_rentals) > 0:
                    anomalies.append({
                        'type': 'property_market',
                        'severity': 'medium',
                        'description': f'{len(extreme_rentals)} properties with extremely high rental prices',
                        'affected_metrics': ['rental_prices'],
                        'data_context': {
                            'extreme_count': len(extreme_rentals),
                            'threshold': upper_bound,
                            'max_rental': extreme_rentals['median_rental_psf'].max()
                        }
                    })
        
        except Exception as e:
            self.logger.error(f"Error detecting property anomalies: {e}")
        
        return anomalies
    
    def _enhance_anomaly_with_llm(self, anomaly: Dict[str, Any]) -> EnhancedAnomalyAlert:
        """Enhance anomaly with LLM explanation"""
        llm_explanation = ""
        potential_causes = []
        monitoring_requirements = []
        recommended_actions = []
        
        try:
            if self.llm_client and self.llm_client.is_available():
                context_data = {
                    "anomaly_details": anomaly,
                    "detection_context": "Singapore economic data analysis"
                }
                
                llm_explanation = self.llm_client.generate_analysis(
                    self.prompts.anomaly_detection_prompt(),
                    context_data
                )
                
                # Extract structured information from LLM response
                potential_causes, monitoring_requirements, recommended_actions = self._parse_llm_anomaly_response(llm_explanation)
            
            else:
                # Fallback explanations
                llm_explanation = self._generate_anomaly_explanation_fallback(anomaly)
                potential_causes = ["Data quality issues", "Market volatility", "Policy changes"]
                monitoring_requirements = ["Daily data quality checks", "Trend monitoring", "Threshold alerts"]
                recommended_actions = ["Investigate data sources", "Validate with domain experts", "Implement monitoring"]
        
        except Exception as e:
            self.logger.error(f"Error enhancing anomaly with LLM: {e}")
            llm_explanation = "LLM explanation unavailable"
        
        return EnhancedAnomalyAlert(
            alert_type=anomaly['type'],
            severity=anomaly['severity'],
            description=anomaly['description'],
            llm_explanation=llm_explanation,
            affected_metrics=anomaly['affected_metrics'],
            timestamp=datetime.now(),
            confidence_score=0.85 if self.llm_client and self.llm_client.is_available() else 0.70,
            recommended_actions=recommended_actions,
            potential_causes=potential_causes,
            monitoring_requirements=monitoring_requirements
        )
    
    def _parse_llm_anomaly_response(self, response: str) -> Tuple[List[str], List[str], List[str]]:
        """Parse LLM response to extract structured information"""
        potential_causes = []
        monitoring_requirements = []
        recommended_actions = []
        
        # Simple parsing logic (could be enhanced with NLP)
        lines = response.split('\n')
        current_section = None
        
        for line in lines:
            line = line.strip()
            
            if 'cause' in line.lower():
                current_section = 'causes'
            elif 'monitor' in line.lower():
                current_section = 'monitoring'
            elif 'action' in line.lower() or 'recommend' in line.lower():
                current_section = 'actions'
            
            if line.startswith(('-', '•', '*')) or (len(line) > 0 and line[0].isdigit()):
                clean_line = line.lstrip('-•*0123456789. ').strip()
                if clean_line:
                    if current_section == 'causes':
                        potential_causes.append(clean_line)
                    elif current_section == 'monitoring':
                        monitoring_requirements.append(clean_line)
                    elif current_section == 'actions':
                        recommended_actions.append(clean_line)
        
        return potential_causes[:3], monitoring_requirements[:3], recommended_actions[:3]
    
    def _generate_anomaly_explanation_fallback(self, anomaly: Dict[str, Any]) -> str:
        """Generate fallback explanation when LLM is not available"""
        anomaly_type = anomaly['type']
        severity = anomaly['severity']
        description = anomaly['description']
        
        return f"""Anomaly Detection Report (Rule-based):
        
Type: {anomaly_type.title()}
Severity: {severity.title()}
Description: {description}
        
Potential Causes:
        - Data collection irregularities
        - Market condition changes
        - Seasonal variations
        - Policy or regulatory changes
        
Recommended Actions:
        - Verify data source accuracy
        - Cross-reference with external sources
        - Consult domain experts
        - Implement additional monitoring
        
Monitoring Requirements:
        - Set up automated alerts
        - Increase data collection frequency
        - Track related metrics
        - Regular review cycles
        """

class EnhancedEconomicIntelligencePlatform:
    """Main enhanced platform with full LLM integration"""
    
    def __init__(self, data_config: DataSourceConfig = None, llm_config: LLMConfig = None):
        # Initialize data connector
        self.data_connector = SilverLayerConnector(data_config)
        
        # Initialize LLM client
        self.llm_client = None
        if llm_config:
            self.llm_client = create_llm_client(llm_config)
        else:
            try:
                default_config = get_default_config()
                self.llm_client = create_llm_client(default_config)
            except Exception as e:
                logger.warning(f"Could not initialize LLM client: {e}")
        
        # Initialize analyzers
        self.analyzer = EnhancedLLMEconomicAnalyzer(self.data_connector, self.llm_client)
        self.anomaly_detector = EnhancedAnomalyDetector(self.data_connector, self.llm_client)
        
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def generate_comprehensive_intelligence_report(self) -> Dict[str, Any]:
        """Generate comprehensive economic intelligence report with LLM insights"""
        self.logger.info("Generating comprehensive economic intelligence report with LLM enhancement...")
        
        report = {
            "report_timestamp": datetime.now().isoformat(),
            "report_type": "enhanced_economic_intelligence",
            "llm_enabled": self.llm_client is not None and self.llm_client.is_available(),
            "data_sources_status": {},
            "insights": [],
            "anomalies": [],
            "forecasts": {},
            "cross_sector_analysis": {},
            "data_quality_assessment": {},
            "executive_summary": "",
            "strategic_recommendations": [],
            "risk_assessment": []
        }
        
        try:
            # Check data sources status
            report["data_sources_status"] = self._check_data_sources_status()
            
            # Generate insights
            self.logger.info("Generating economic insights...")
            business_insights = self.analyzer.analyze_business_formation_trends()
            economic_insights = self.analyzer.analyze_economic_indicators()
            cross_sector_insights = self.analyzer.analyze_cross_sector_correlations()
            
            all_insights = business_insights + economic_insights + cross_sector_insights
            report["insights"] = [self._insight_to_dict(insight) for insight in all_insights]
            
            # Detect anomalies
            self.logger.info("Detecting anomalies...")
            anomalies = self.anomaly_detector.detect_comprehensive_anomalies()
            report["anomalies"] = [self._anomaly_to_dict(anomaly) for anomaly in anomalies]
            
            # Generate data quality assessment
            report["data_quality_assessment"] = self.data_connector.get_data_summary()
            
            # Generate executive summary and recommendations
            if self.llm_client and self.llm_client.is_available():
                report["executive_summary"] = self._generate_executive_summary(report)
                report["strategic_recommendations"] = self._generate_strategic_recommendations(report)
                report["risk_assessment"] = self._generate_risk_assessment(report)
            else:
                report["executive_summary"] = self._generate_executive_summary_fallback(report)
                report["strategic_recommendations"] = self._generate_recommendations_fallback(report)
                report["risk_assessment"] = self._generate_risk_assessment_fallback(report)
            
            self.logger.info("Comprehensive intelligence report generated successfully")
            
        except Exception as e:
            self.logger.error(f"Error generating comprehensive report: {e}")
            report["error"] = str(e)
        
        return report
    
    def _check_data_sources_status(self) -> Dict[str, Any]:
        """Check status of all data sources"""
        status = {
            "database_connection": self.data_connector.is_connected(),
            "llm_connection": self.llm_client is not None and self.llm_client.is_available(),
            "data_availability": {}
        }
        
        try:
            # Test data loading from each source
            acra_test = self.data_connector.load_acra_companies(limit=1)
            status["data_availability"]["acra_companies"] = not acra_test.empty
            
            econ_test = self.data_connector.load_economic_indicators(limit=1)
            status["data_availability"]["economic_indicators"] = not econ_test.empty
            
            gov_test = self.data_connector.load_government_expenditure(limit=1)
            status["data_availability"]["government_expenditure"] = not gov_test.empty
            
            prop_test = self.data_connector.load_property_market(limit=1)
            status["data_availability"]["property_market"] = not prop_test.empty
            
        except Exception as e:
            self.logger.error(f"Error checking data sources: {e}")
            status["error"] = str(e)
        
        return status
    
    def _generate_executive_summary(self, report: Dict[str, Any]) -> str:
        """Generate executive summary using LLM"""
        try:
            context_data = {
                "insights_count": len(report.get("insights", [])),
                "anomalies_count": len(report.get("anomalies", [])),
                "high_severity_anomalies": len([a for a in report.get("anomalies", []) if a.get("severity") in ["high", "critical"]]),
                "data_quality": report.get("data_quality_assessment", {}),
                "key_insights": [insight.get("title", "") for insight in report.get("insights", [])[:3]]
            }
            
            summary_prompt = """
            Generate an executive summary for Singapore's economic intelligence report. Focus on:
            
            1. Overall economic health assessment
            2. Key trends and patterns identified
            3. Critical issues requiring attention
            4. Strategic implications for policymakers
            5. Data quality and reliability assessment
            
            Keep the summary concise, actionable, and suitable for senior decision-makers.
            """
            
            return self.llm_client.generate_analysis(summary_prompt, context_data)
            
        except Exception as e:
            self.logger.error(f"Error generating executive summary: {e}")
            return self._generate_executive_summary_fallback(report)
    
    def _generate_strategic_recommendations(self, report: Dict[str, Any]) -> List[str]:
        """Generate strategic recommendations using LLM"""
        try:
            context_data = {
                "insights": report.get("insights", []),
                "anomalies": report.get("anomalies", []),
                "data_quality": report.get("data_quality_assessment", {})
            }
            
            recommendations_prompt = """
            Based on the economic intelligence analysis, provide strategic recommendations for Singapore's economic development. Focus on:
            
            1. Policy interventions to address identified issues
            2. Economic diversification opportunities
            3. Risk mitigation strategies
            4. Data and monitoring improvements
            5. International competitiveness enhancement
            
            Provide specific, actionable recommendations with clear priorities.
            """
            
            response = self.llm_client.generate_analysis(recommendations_prompt, context_data)
            
            # Extract recommendations from response
            recommendations = []
            lines = response.split('\n')
            for line in lines:
                line = line.strip()
                if line.startswith(('-', '•', '*')) or (len(line) > 0 and line[0].isdigit()):
                    clean_line = line.lstrip('-•*0123456789. ').strip()
                    if clean_line:
                        recommendations.append(clean_line)
            
            return recommendations[:10]  # Top 10 recommendations
            
        except Exception as e:
            self.logger.error(f"Error generating strategic recommendations: {e}")
            return self._generate_recommendations_fallback(report)
    
    def _generate_risk_assessment(self, report: Dict[str, Any]) -> List[str]:
        """Generate risk assessment using LLM"""
        try:
            context_data = {
                "anomalies": report.get("anomalies", []),
                "insights": report.get("insights", []),
                "data_quality": report.get("data_quality_assessment", {})
            }
            
            risk_prompt = """
            Assess the economic risks for Singapore based on the analysis. Focus on:
            
            1. Immediate risks requiring urgent attention
            2. Medium-term structural risks
            3. External economic vulnerabilities
            4. Data quality and monitoring risks
            5. Policy implementation risks
            
            Prioritize risks by severity and likelihood, providing clear risk descriptions.
            """
            
            response = self.llm_client.generate_analysis(risk_prompt, context_data)
            
            # Extract risks from response
            risks = []
            lines = response.split('\n')
            for line in lines:
                line = line.strip()
                if line.startswith(('-', '•', '*')) or (len(line) > 0 and line[0].isdigit()):
                    clean_line = line.lstrip('-•*0123456789. ').strip()
                    if clean_line:
                        risks.append(clean_line)
            
            return risks[:8]  # Top 8 risks
            
        except Exception as e:
            self.logger.error(f"Error generating risk assessment: {e}")
            return self._generate_risk_assessment_fallback(report)
    
    def _generate_executive_summary_fallback(self, report: Dict[str, Any]) -> str:
        """Generate fallback executive summary"""
        insights_count = len(report.get("insights", []))
        anomalies_count = len(report.get("anomalies", []))
        high_severity = len([a for a in report.get("anomalies", []) if a.get("severity") in ["high", "critical"]])
        
        return f"""Executive Summary (Rule-based):
        
Singapore Economic Intelligence Report - {datetime.now().strftime('%B %Y')}
        
Key Findings:
        - Generated {insights_count} economic insights across multiple sectors
        - Identified {anomalies_count} anomalies, including {high_severity} high-severity alerts
        - Data quality assessment shows comprehensive coverage across key economic indicators
        
Overall Assessment:
        - Economic fundamentals remain stable with areas for continued monitoring
        - Business formation trends indicate healthy economic activity
        - Cross-sector analysis reveals interconnected growth patterns
        
Immediate Priorities:
        - Address high-severity anomalies requiring urgent attention
        - Enhance data quality monitoring across all sources
        - Strengthen cross-sector policy coordination
        
Strategic Outlook:
        - Continue diversification efforts to maintain economic resilience
        - Leverage data-driven insights for proactive policy development
        - Maintain focus on innovation and technology sector growth
        """
    
    def _generate_recommendations_fallback(self, report: Dict[str, Any]) -> List[str]:
        """Generate fallback recommendations"""
        return [
            "Enhance real-time economic monitoring capabilities",
            "Strengthen data integration across government agencies",
            "Develop sector-specific policy intervention frameworks",
            "Improve business formation support programs",
            "Expand economic diversification initiatives",
            "Implement predictive analytics for early warning systems",
            "Enhance international economic cooperation",
            "Strengthen digital economy infrastructure"
        ]
    
    def _generate_risk_assessment_fallback(self, report: Dict[str, Any]) -> List[str]:
        """Generate fallback risk assessment"""
        return [
            "Economic concentration risks in specific sectors",
            "Data quality degradation affecting analysis accuracy",
            "External economic shocks and global uncertainties",
            "Policy coordination challenges across agencies",
            "Technology disruption impacts on traditional industries",
            "Inflation pressures affecting business costs",
            "Talent shortage in key growth sectors",
            "Infrastructure capacity constraints"
        ]
    
    def _insight_to_dict(self, insight: EnhancedEconomicInsight) -> Dict[str, Any]:
        """Convert EnhancedEconomicInsight to dictionary"""
        return asdict(insight)
    
    def _anomaly_to_dict(self, anomaly: EnhancedAnomalyAlert) -> Dict[str, Any]:
        """Convert EnhancedAnomalyAlert to dictionary"""
        return asdict(anomaly)
    
    def save_report(self, report: Dict[str, Any], output_path: str = None) -> str:
        """Save enhanced report to file"""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"/tmp/enhanced_economic_intelligence_report_{timestamp}.json"
        
        try:
            # Convert datetime objects to strings for JSON serialization
            def convert_datetime(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
            
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2, default=convert_datetime)
            
            self.logger.info(f"Enhanced report saved to {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Error saving enhanced report: {e}")
            raise
    
    def close(self):
        """Clean up resources"""
        if self.data_connector:
            self.data_connector.close_connection()

def main():
    """Main function for testing the enhanced economic intelligence platform"""
    print("🧠 Enhanced LLM-Based Economic Intelligence Platform")
    print("=" * 60)
    
    # Initialize platform
    print("\n🔧 Initializing platform...")
    platform = EnhancedEconomicIntelligencePlatform()
    
    # Check system status
    print("\n📊 System Status:")
    print(f"   Database Connection: {'✅' if platform.data_connector.is_connected() else '❌'}")
    print(f"   LLM Integration: {'✅' if platform.llm_client and platform.llm_client.is_available() else '❌'}")
    
    # Generate comprehensive report
    print("\n📈 Generating comprehensive economic intelligence report...")
    report = platform.generate_comprehensive_intelligence_report()
    
    # Display summary
    print(f"\n✅ Enhanced report generated with:")
    print(f"   - {len(report.get('insights', []))} economic insights")
    print(f"   - {len(report.get('anomalies', []))} anomaly alerts")
    print(f"   - {len(report.get('strategic_recommendations', []))} strategic recommendations")
    print(f"   - {len(report.get('risk_assessment', []))} risk factors")
    print(f"   - LLM Enhancement: {'Enabled' if report.get('llm_enabled') else 'Disabled'}")
    
    # Save report
    output_path = platform.save_report(report)
    print(f"\n💾 Enhanced report saved to: {output_path}")
    
    # Display executive summary
    if report.get('executive_summary'):
        print("\n📋 Executive Summary:")
        summary_lines = report['executive_summary'].split('\n')[:5]  # First 5 lines
        for line in summary_lines:
            if line.strip():
                print(f"   {line.strip()}")
        if len(report['executive_summary'].split('\n')) > 5:
            print("   ...")
    
    # Display top recommendations
    if report.get('strategic_recommendations'):
        print("\n🎯 Top Strategic Recommendations:")
        for i, rec in enumerate(report['strategic_recommendations'][:3], 1):
            print(f"   {i}. {rec}")
    
    # Display critical anomalies
    critical_anomalies = [a for a in report.get('anomalies', []) if a.get('severity') == 'critical']
    if critical_anomalies:
        print("\n🚨 Critical Anomalies:")
        for anomaly in critical_anomalies:
            print(f"   • {anomaly.get('description', 'Unknown anomaly')}")
    
    print("\n🎯 Next Steps:")
    print("   1. Review executive summary and strategic recommendations")
    print("   2. Address critical anomalies immediately")
    print("   3. Implement enhanced monitoring for identified risks")
    print("   4. Set up automated report generation schedule")
    print("   5. Integrate with visualization dashboard")
    
    # Clean up
    platform.close()
    print("\n✅ Platform shutdown complete")

if __name__ == "__main__":
    main()