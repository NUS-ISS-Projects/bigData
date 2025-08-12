#!/usr/bin/env python3
"""
Enhanced Visual Economic Intelligence Platform
Adds comprehensive charts, graphs, and visual analytics to economic intelligence reports
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
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

# Import our custom modules
from analytics.enhanced_economic_intelligence import EnhancedEconomicIntelligencePlatform
from analytics.silver_data_connector import SilverLayerConnector, DataSourceConfig
from analytics.llm_config import LLMConfig, LLMProvider, get_default_config, create_llm_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class VisualEconomicAnalyzer:
    """Enhanced economic analyzer with comprehensive visualizations"""
    
    def __init__(self, data_connector: SilverLayerConnector):
        self.data_connector = data_connector
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def create_comprehensive_dashboard_charts(self) -> Dict[str, Any]:
        """Create comprehensive set of charts for economic intelligence dashboard"""
        charts = {}
        
        try:
            # Load all data sources individually since load_all_data doesn't exist
            data = {
                'acra': self.data_connector.load_acra_companies(),
                'economic': self.data_connector.load_economic_indicators(),
                'government': self.data_connector.load_government_expenditure(),
                'property': self.data_connector.load_property_market(),
                'commercial': self.data_connector.load_commercial_rental()
            }
            
            self.logger.info(f"Loaded data: {[(k, len(v)) for k, v in data.items()]}")
            
            # 1. Business Formation Analytics
            if not data['acra'].empty:
                charts['business_formation'] = self._create_business_formation_charts(data['acra'])
            
            # 2. Economic Indicators Dashboard
            if not data['economic'].empty:
                charts['economic_indicators'] = self._create_economic_indicators_charts(data['economic'])
            
            # 3. Government Spending Analysis
            if not data['government'].empty:
                charts['government_spending'] = self._create_government_spending_charts(data['government'])
            
            # 4. Property Market Intelligence
            if not data['property'].empty:
                charts['property_market'] = self._create_property_market_charts(data['property'])
            
            # 5. Cross-Sector Correlation Analysis
            charts['cross_sector'] = self._create_cross_sector_charts(data)
            
            # 6. Risk Assessment Visualizations
            charts['risk_assessment'] = self._create_risk_assessment_charts(data)
            
            # 7. Executive Summary Metrics
            charts['executive_metrics'] = self._create_executive_summary_charts(data)
            
            self.logger.info(f"Generated {len(charts)} chart categories with comprehensive visualizations")
            
        except Exception as e:
            self.logger.error(f"Error creating comprehensive charts: {e}", exc_info=True)
            
        return charts
    
    def _create_business_formation_charts(self, acra_data: pd.DataFrame) -> Dict[str, Any]:
        """Create comprehensive business formation analysis charts with enhanced insights"""
        charts = {}
        
        if acra_data.empty:
            return charts
            
        try:
            # 1. Geographic Distribution Analysis
            if 'reg_postal_code' in acra_data.columns:
                # Group by postal code regions (first 2 digits)
                acra_data['postal_region'] = acra_data['reg_postal_code'].astype(str).str[:2]
                region_counts = acra_data['postal_region'].value_counts().head(15)
                charts['geographic_distribution'] = {
                    'type': 'pie',
                    'data': {
                        'labels': [f'Region {region}' for region in region_counts.index.tolist()],
                        'values': region_counts.values.tolist()
                    },
                    'title': 'Top 15 Regions by Company Formation',
                    'description': 'Geographic distribution of new business formations by postal regions'
                }
                
                # 2. Regional Business Density Heatmap Data
                region_density = acra_data.groupby('postal_region').agg({
                    'uen': 'count',
                    'entity_type': lambda x: x.nunique(),
                    'entity_status': lambda x: (x == 'REGISTERED').sum()
                }).rename(columns={'uen': 'total_companies', 'entity_type': 'entity_diversity', 'entity_status': 'active_companies'})
                
                top_regions = region_density.nlargest(20, 'total_companies')
                charts['regional_business_density'] = {
                    'type': 'heatmap',
                    'data': {
                        'regions': [f'Region {idx}' for idx in top_regions.index.tolist()],
                        'total_companies': top_regions['total_companies'].tolist(),
                        'entity_diversity': top_regions['entity_diversity'].tolist(),
                        'active_companies': top_regions['active_companies'].tolist()
                    },
                    'title': 'Regional Business Formation Density Analysis',
                    'description': 'Comprehensive view of business activity across Singapore regions'
                }
            
            # 3. Enhanced Entity Type Analysis with Growth Metrics
            if 'entity_type' in acra_data.columns:
                entity_counts = acra_data['entity_type'].value_counts()
                entity_active = acra_data[acra_data['entity_status'] == 'REGISTERED']['entity_type'].value_counts() if 'entity_status' in acra_data.columns else entity_counts
                
                charts['entity_type_distribution'] = {
                    'type': 'bar',
                    'data': {
                        'x': entity_counts.index.tolist(),
                        'y': entity_counts.values.tolist()
                    },
                    'title': 'Business Entity Types Distribution',
                    'description': 'Breakdown of companies by legal entity structure'
                }
                
                # Entity Type Performance Analysis
                if 'entity_status' in acra_data.columns:
                    entity_performance = acra_data.groupby('entity_type')['entity_status'].apply(
                        lambda x: (x == 'REGISTERED').sum() / len(x) * 100
                    ).round(2)
                    
                    charts['entity_performance_analysis'] = {
                        'type': 'horizontal_bar',
                        'data': {
                            'x': entity_performance.values.tolist(),
                            'y': entity_performance.index.tolist()
                        },
                        'title': 'Entity Type Success Rate (% Active)',
                        'description': 'Percentage of active companies by entity type'
                    }
            
            # 4. Company Status Analysis with Detailed Breakdown
            if 'entity_status' in acra_data.columns:
                status_counts = acra_data['entity_status'].value_counts()
                charts['company_status'] = {
                    'type': 'donut',
                    'data': {
                        'labels': status_counts.index.tolist(),
                        'values': status_counts.values.tolist()
                    },
                    'title': 'Company Status Distribution',
                    'description': 'Current operational status of registered companies'
                }
            
            # 5. Enhanced Registration Trends with Seasonal Analysis
            if 'uen_issue_date' in acra_data.columns:
                acra_data['uen_issue_date'] = pd.to_datetime(acra_data['uen_issue_date'], errors='coerce')
                
                # Monthly trends
                monthly_registrations = acra_data.groupby(acra_data['uen_issue_date'].dt.to_period('M')).size()
                charts['registration_trends'] = {
                    'type': 'line',
                    'data': {
                        'x': [str(period) for period in monthly_registrations.index],
                        'y': monthly_registrations.values.tolist()
                    },
                    'title': 'Monthly Business Registration Trends',
                    'description': 'Timeline of new business formations over time'
                }
                
                # Yearly registration analysis
                yearly_registrations = acra_data.groupby(acra_data['uen_issue_date'].dt.year).size()
                if len(yearly_registrations) > 1:
                    charts['yearly_registration_growth'] = {
                        'type': 'bar',
                        'data': {
                            'x': yearly_registrations.index.tolist(),
                            'y': yearly_registrations.values.tolist()
                        },
                        'title': 'Annual Business Registration Volume',
                        'description': 'Year-over-year business formation trends'
                    }
                
                # Seasonal patterns
                acra_data['month'] = acra_data['uen_issue_date'].dt.month
                seasonal_patterns = acra_data.groupby('month').size()
                month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                charts['seasonal_registration_patterns'] = {
                    'type': 'radar',
                    'data': {
                        'labels': [month_names[i-1] for i in seasonal_patterns.index],
                        'values': seasonal_patterns.values.tolist()
                    },
                    'title': 'Seasonal Business Registration Patterns',
                    'description': 'Monthly distribution of business formations throughout the year'
                }
            
            # 6. Data Quality and Coverage Analysis
            total_records = len(acra_data)
            data_quality_metrics = {
                'total_companies': total_records,
                'unique_entities': acra_data['uen'].nunique() if 'uen' in acra_data.columns else total_records,
                'data_completeness': {
                    'entity_name': acra_data['entity_name'].notna().sum() / total_records * 100 if 'entity_name' in acra_data.columns else 0,
                    'postal_code': acra_data['reg_postal_code'].notna().sum() / total_records * 100 if 'reg_postal_code' in acra_data.columns else 0,
                    'entity_type': acra_data['entity_type'].notna().sum() / total_records * 100 if 'entity_type' in acra_data.columns else 0
                }
            }
            
            charts['data_quality_overview'] = {
                'type': 'metrics',
                'data': data_quality_metrics,
                'title': 'Business Formation Data Quality Overview',
                'description': 'Comprehensive data quality and coverage metrics'
            }
            
            # 7. Business Formation Velocity Analysis
            if 'uen_issue_date' in acra_data.columns:
                # Calculate recent trends (last 12 months)
                recent_data = acra_data[acra_data['uen_issue_date'] >= (datetime.now() - timedelta(days=365))]
                if not recent_data.empty:
                    recent_monthly = recent_data.groupby(recent_data['uen_issue_date'].dt.to_period('M')).size()
                    
                    charts['recent_formation_velocity'] = {
                        'type': 'area',
                        'data': {
                            'x': [str(period) for period in recent_monthly.index],
                            'y': recent_monthly.values.tolist()
                        },
                        'title': 'Recent Business Formation Velocity (Last 12 Months)',
                        'description': 'Recent trends in business formation activity'
                    }
                
        except Exception as e:
            self.logger.error(f"Error creating business formation charts: {e}")
            
        return charts
    
    def _create_economic_indicators_charts(self, econ_data: pd.DataFrame) -> Dict[str, Any]:
        """Create economic indicators visualization charts"""
        charts = {}
        
        if econ_data.empty:
            return charts
            
        try:
            # 1. Key Economic Indicators Dashboard
            if 'table_id' in econ_data.columns and 'value_numeric' in econ_data.columns:
                latest_indicators = econ_data.groupby('table_id')['value_numeric'].last().head(15)
                charts['key_indicators'] = {
                    'type': 'horizontal_bar',
                    'data': {
                        'x': latest_indicators.values.tolist(),
                        'y': latest_indicators.index.tolist()
                    },
                    'title': 'Latest Economic Indicators',
                    'description': 'Current values of key economic performance metrics'
                }
            
            # 2. Economic Indicator Categories
            if 'category' in econ_data.columns:
                category_counts = econ_data['category'].value_counts()
                charts['indicator_categories'] = {
                    'type': 'bar',
                    'data': {
                        'x': category_counts.index.tolist(),
                        'y': category_counts.values.tolist()
                    },
                    'title': 'Economic Indicator Categories',
                    'description': 'Distribution of indicators across economic sectors'
                }
            
            # 3. Time Series Analysis (if period data available)
            if 'period' in econ_data.columns and 'value_numeric' in econ_data.columns:
                # Get top 5 indicators for time series
                top_indicators = econ_data['table_id'].value_counts().head(5).index
                time_series_data = []
                
                for indicator in top_indicators:
                    indicator_data = econ_data[econ_data['table_id'] == indicator]
                    time_series_data.append({
                        'name': indicator,
                        'x': indicator_data['period'].tolist(),
                        'y': indicator_data['value_numeric'].tolist()
                    })
                
                charts['economic_trends'] = {
                    'type': 'multi_line',
                    'data': time_series_data,
                    'title': 'Economic Indicators Trends',
                    'description': 'Historical trends of key economic indicators'
                }
                
        except Exception as e:
            self.logger.error(f"Error creating economic indicators charts: {e}")
            
        return charts
    
    def _create_government_spending_charts(self, gov_data: pd.DataFrame) -> Dict[str, Any]:
        """Create government spending analysis charts"""
        charts = {}
        
        if gov_data.empty:
            return charts
            
        try:
            # 1. Spending by Category
            if 'category' in gov_data.columns and 'amount_million_numeric' in gov_data.columns:
                category_spending = gov_data.groupby('category')['amount_million_numeric'].sum().sort_values(ascending=False)
                charts['spending_by_category'] = {
                    'type': 'pie',
                    'data': {
                        'labels': category_spending.index.tolist(),
                        'values': category_spending.values.tolist()
                    },
                    'title': 'Government Spending by Category (Million SGD)',
                    'description': 'Distribution of government expenditure across different categories'
                }
            
            # 2. Spending Trends Over Time
            if 'fiscal_year' in gov_data.columns and 'amount_million_numeric' in gov_data.columns:
                yearly_spending = gov_data.groupby('fiscal_year')['amount_million_numeric'].sum()
                charts['spending_trends'] = {
                    'type': 'line',
                    'data': {
                        'x': yearly_spending.index.tolist(),
                        'y': yearly_spending.values.tolist()
                    },
                    'title': 'Government Spending Trends by Fiscal Year',
                    'description': 'Annual government expenditure patterns'
                }
            
            # 3. Top Spending Areas
            if 'description' in gov_data.columns and 'amount_million_numeric' in gov_data.columns:
                top_spending = gov_data.groupby('description')['amount_million_numeric'].sum().sort_values(ascending=False).head(10)
                charts['top_spending_areas'] = {
                    'type': 'horizontal_bar',
                    'data': {
                        'x': top_spending.values.tolist(),
                        'y': top_spending.index.tolist()
                    },
                    'title': 'Top 10 Government Spending Areas',
                    'description': 'Highest government expenditure categories'
                }
                
        except Exception as e:
            self.logger.error(f"Error creating government spending charts: {e}")
            
        return charts
    
    def _create_property_market_charts(self, prop_data: pd.DataFrame) -> Dict[str, Any]:
        """Create property market analysis charts"""
        charts = {}
        
        if prop_data.empty:
            return charts
            
        try:
            # 1. Rental Price Distribution
            if 'rental_median' in prop_data.columns:
                valid_rentals = prop_data[prop_data['rental_median'].notna()]['rental_median']
                charts['rental_distribution'] = {
                    'type': 'histogram',
                    'data': {
                        'x': valid_rentals.tolist(),
                        'nbins': 30
                    },
                    'title': 'Property Rental Price Distribution',
                    'description': 'Distribution of median rental prices across properties'
                }
            
            # 2. Property Types Analysis
            if 'property_type' in prop_data.columns:
                type_counts = prop_data['property_type'].value_counts()
                charts['property_types'] = {
                    'type': 'donut',
                    'data': {
                        'labels': type_counts.index.tolist(),
                        'values': type_counts.values.tolist()
                    },
                    'title': 'Property Types Distribution',
                    'description': 'Breakdown of properties by type'
                }
            
            # 3. District-wise Analysis
            if 'district' in prop_data.columns and 'rental_median' in prop_data.columns:
                district_rentals = prop_data.groupby('district')['rental_median'].mean().sort_values(ascending=False).head(15)
                charts['district_rentals'] = {
                    'type': 'bar',
                    'data': {
                        'x': district_rentals.index.tolist(),
                        'y': district_rentals.values.tolist()
                    },
                    'title': 'Average Rental Prices by District',
                    'description': 'Median rental prices across different districts'
                }
            
            # 4. Price Range Analysis
            if 'rental_median' in prop_data.columns:
                valid_data = prop_data[prop_data['rental_median'].notna()]
                if not valid_data.empty:
                    price_ranges = pd.cut(valid_data['rental_median'], bins=5, labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])
                    range_counts = price_ranges.value_counts()
                    charts['price_ranges'] = {
                        'type': 'bar',
                        'data': {
                            'x': range_counts.index.tolist(),
                            'y': range_counts.values.tolist()
                        },
                        'title': 'Property Price Range Distribution',
                        'description': 'Properties categorized by rental price ranges'
                    }
                    
        except Exception as e:
            self.logger.error(f"Error creating property market charts: {e}")
            
        return charts
    
    def _create_cross_sector_charts(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Create cross-sector correlation analysis charts"""
        charts = {}
        
        try:
            # 1. Data Source Coverage Comparison
            data_coverage = {}
            for source, df in data.items():
                data_coverage[source.replace('_', ' ').title()] = len(df) if not df.empty else 0
            
            charts['data_coverage'] = {
                'type': 'bar',
                'data': {
                    'x': list(data_coverage.keys()),
                    'y': list(data_coverage.values())
                },
                'title': 'Data Coverage Across Economic Sectors',
                'description': 'Number of records available for each economic data source'
            }
            
            # 2. Sector Health Scorecard
            sector_scores = self._calculate_sector_health_scores(data)
            charts['sector_health'] = {
                'type': 'radar',
                'data': {
                    'categories': list(sector_scores.keys()),
                    'values': list(sector_scores.values())
                },
                'title': 'Economic Sector Health Scorecard',
                'description': 'Relative health assessment across different economic sectors'
            }
            
            # 3. Economic Diversity Index
            diversity_metrics = self._calculate_diversity_metrics(data)
            charts['economic_diversity'] = {
                'type': 'gauge',
                'data': {
                    'value': diversity_metrics['overall_diversity'],
                    'max': 100
                },
                'title': 'Economic Diversity Index',
                'description': 'Overall economic diversification score'
            }
            
        except Exception as e:
            self.logger.error(f"Error creating cross-sector charts: {e}")
            
        return charts
    
    def _create_risk_assessment_charts(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Create risk assessment visualization charts"""
        charts = {}
        
        try:
            # 1. Risk Level Distribution
            risk_levels = self._assess_sector_risks(data)
            charts['risk_distribution'] = {
                'type': 'pie',
                'data': {
                    'labels': list(risk_levels.keys()),
                    'values': list(risk_levels.values())
                },
                'title': 'Economic Risk Level Distribution',
                'description': 'Distribution of risk levels across economic sectors'
            }
            
            # 2. Risk Trend Analysis
            risk_trends = self._calculate_risk_trends(data)
            charts['risk_trends'] = {
                'type': 'line',
                'data': {
                    'x': list(range(len(risk_trends))),
                    'y': risk_trends
                },
                'title': 'Economic Risk Trend Analysis',
                'description': 'Historical trend of overall economic risk levels'
            }
            
        except Exception as e:
            self.logger.error(f"Error creating risk assessment charts: {e}")
            
        return charts
    
    def _create_executive_summary_charts(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Create executive summary key metrics charts"""
        charts = {}
        
        try:
            # 1. Key Performance Indicators
            kpis = self._calculate_key_performance_indicators(data)
            charts['kpi_dashboard'] = {
                'type': 'metric_cards',
                'data': kpis,
                'title': 'Key Performance Indicators',
                'description': 'Critical economic performance metrics at a glance'
            }
            
            # 2. Economic Health Summary
            health_summary = self._calculate_economic_health_summary(data)
            charts['health_summary'] = {
                'type': 'speedometer',
                'data': {
                    'value': health_summary['overall_score'],
                    'max': 100,
                    'zones': [
                        {'min': 0, 'max': 30, 'color': 'red', 'label': 'Poor'},
                        {'min': 30, 'max': 70, 'color': 'yellow', 'label': 'Moderate'},
                        {'min': 70, 'max': 100, 'color': 'green', 'label': 'Good'}
                    ]
                },
                'title': 'Overall Economic Health Score',
                'description': 'Composite score of economic performance across all sectors'
            }
            
        except Exception as e:
            self.logger.error(f"Error creating executive summary charts: {e}")
            
        return charts
    
    def _calculate_sector_health_scores(self, data: Dict[str, pd.DataFrame]) -> Dict[str, float]:
        """Calculate health scores for each economic sector"""
        scores = {}
        
        # Business Formation Health (based on diversity and activity)
        if not data['acra'].empty:
            industry_diversity = data['acra']['primary_ssic_description'].nunique() if 'primary_ssic_description' in data['acra'].columns else 0
            scores['Business Formation'] = min(100, (industry_diversity / 50) * 100)  # Normalize to 100
        
        # Economic Indicators Health
        if not data['economic'].empty:
            indicator_count = data['economic']['table_id'].nunique() if 'table_id' in data['economic'].columns else 0
            scores['Economic Indicators'] = min(100, (indicator_count / 20) * 100)
        
        # Government Spending Health
        if not data['government'].empty:
            spending_diversity = data['government']['category'].nunique() if 'category' in data['government'].columns else 0
            scores['Government Spending'] = min(100, (spending_diversity / 10) * 100)
        
        # Property Market Health
        if not data['property'].empty:
            property_diversity = data['property']['property_type'].nunique() if 'property_type' in data['property'].columns else 0
            scores['Property Market'] = min(100, (property_diversity / 15) * 100)
        
        return scores
    
    def _calculate_diversity_metrics(self, data: Dict[str, pd.DataFrame]) -> Dict[str, float]:
        """Calculate economic diversity metrics"""
        metrics = {}
        
        total_diversity = 0
        sector_count = 0
        
        for sector, df in data.items():
            if not df.empty:
                sector_count += 1
                # Calculate diversity based on unique categories in each sector
                if sector == 'acra' and 'primary_ssic_description' in df.columns:
                    total_diversity += df['primary_ssic_description'].nunique()
                elif sector == 'economic' and 'table_id' in df.columns:
                    total_diversity += df['table_id'].nunique()
                elif sector == 'government' and 'category' in df.columns:
                    total_diversity += df['category'].nunique()
                elif sector == 'property' and 'property_type' in df.columns:
                    total_diversity += df['property_type'].nunique()
        
        metrics['overall_diversity'] = min(100, (total_diversity / 100) * 100) if sector_count > 0 else 0
        metrics['sector_coverage'] = (sector_count / 4) * 100
        
        return metrics
    
    def _assess_sector_risks(self, data: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """Assess risk levels across sectors"""
        risk_levels = {'Low Risk': 0, 'Medium Risk': 0, 'High Risk': 0}
        
        for sector, df in data.items():
            if df.empty:
                risk_levels['High Risk'] += 1
            elif len(df) < 100:
                risk_levels['Medium Risk'] += 1
            else:
                risk_levels['Low Risk'] += 1
        
        return risk_levels
    
    def _calculate_risk_trends(self, data: Dict[str, pd.DataFrame]) -> List[float]:
        """Calculate risk trend over time (simplified)"""
        # Simplified risk calculation based on data availability
        risk_scores = []
        for i in range(12):  # 12 months
            base_risk = 50  # Baseline risk
            data_quality_factor = sum(1 for df in data.values() if not df.empty) * 10
            monthly_risk = max(0, min(100, base_risk - data_quality_factor + (i * 2)))
            risk_scores.append(monthly_risk)
        
        return risk_scores
    
    def _calculate_key_performance_indicators(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Calculate key performance indicators"""
        kpis = {}
        
        # Total Companies
        kpis['Total Companies'] = {
            'value': len(data['acra']) if not data['acra'].empty else 0,
            'unit': 'companies',
            'trend': 'up'  # Simplified
        }
        
        # Economic Indicators Count
        kpis['Economic Indicators'] = {
            'value': data['economic']['table_id'].nunique() if not data['economic'].empty and 'table_id' in data['economic'].columns else 0,
            'unit': 'indicators',
            'trend': 'stable'
        }
        
        # Government Spending
        total_spending = data['government']['amount_million_numeric'].sum() if not data['government'].empty and 'amount_million_numeric' in data['government'].columns else 0
        kpis['Government Spending'] = {
            'value': total_spending,
            'unit': 'M SGD',
            'trend': 'up'
        }
        
        # Property Records
        kpis['Property Records'] = {
            'value': len(data['property']) if not data['property'].empty else 0,
            'unit': 'properties',
            'trend': 'stable'
        }
        
        return kpis
    
    def _calculate_economic_health_summary(self, data: Dict[str, pd.DataFrame]) -> Dict[str, float]:
        """Calculate overall economic health summary"""
        health_factors = []
        
        # Data availability factor
        data_availability = sum(1 for df in data.values() if not df.empty) / 4 * 100
        health_factors.append(data_availability)
        
        # Data volume factor
        total_records = sum(len(df) for df in data.values())
        data_volume_score = min(100, (total_records / 10000) * 100)
        health_factors.append(data_volume_score)
        
        # Diversity factor
        diversity_scores = self._calculate_sector_health_scores(data)
        avg_diversity = sum(diversity_scores.values()) / len(diversity_scores) if diversity_scores else 0
        health_factors.append(avg_diversity)
        
        overall_score = sum(health_factors) / len(health_factors) if health_factors else 0
        
        return {
            'overall_score': round(overall_score, 1),
            'data_availability': round(data_availability, 1),
            'data_volume': round(data_volume_score, 1),
            'sector_diversity': round(avg_diversity, 1)
        }

class EnhancedVisualIntelligencePlatform:
    """Enhanced platform that combines intelligence reports with comprehensive visualizations"""
    
    def __init__(self, data_config: DataSourceConfig = None, llm_config: LLMConfig = None):
        self.data_config = data_config or DataSourceConfig()
        self.llm_config = llm_config or get_default_config()
        self.data_connector = SilverLayerConnector(self.data_config)
        self.visual_analyzer = VisualEconomicAnalyzer(self.data_connector)
        self.intelligence_platform = EnhancedEconomicIntelligencePlatform(data_config, llm_config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def generate_enhanced_visual_report(self) -> Dict[str, Any]:
        """Generate comprehensive intelligence report with visualizations"""
        self.logger.info("Generating enhanced visual intelligence report...")
        
        try:
            # Generate base intelligence report
            base_report = self.intelligence_platform.generate_comprehensive_intelligence_report()
            
            # Generate comprehensive visualizations
            visual_charts = self.visual_analyzer.create_comprehensive_dashboard_charts()
            
            # Combine reports
            enhanced_report = {
                **base_report,
                'visualizations': visual_charts,
                'report_type': 'enhanced_visual_intelligence',
                'visualization_count': sum(len(category) for category in visual_charts.values()),
                'enhancement_timestamp': datetime.now().isoformat()
            }
            
            # Add visual insights
            enhanced_report['visual_insights'] = self._generate_visual_insights(visual_charts)
            
            # Add chart recommendations
            enhanced_report['chart_recommendations'] = self._generate_chart_recommendations(visual_charts)
            
            self.logger.info(f"Enhanced visual report generated with {enhanced_report['visualization_count']} visualizations")
            
            return enhanced_report
            
        except Exception as e:
            self.logger.error(f"Error generating enhanced visual report: {e}")
            return {}
    
    def _generate_visual_insights(self, charts: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate insights based on visual data analysis"""
        insights = []
        
        try:
            # Business Formation Insights
            if 'business_formation' in charts:
                insights.append({
                    'category': 'Business Formation',
                    'insight': 'Industry diversity analysis reveals concentration patterns in business formations',
                    'chart_reference': 'business_formation.industry_distribution',
                    'actionable': True
                })
            
            # Economic Indicators Insights
            if 'economic_indicators' in charts:
                insights.append({
                    'category': 'Economic Performance',
                    'insight': 'Key economic indicators show current performance across multiple sectors',
                    'chart_reference': 'economic_indicators.key_indicators',
                    'actionable': True
                })
            
            # Government Spending Insights
            if 'government_spending' in charts:
                insights.append({
                    'category': 'Government Investment',
                    'insight': 'Government spending patterns indicate priority areas for economic development',
                    'chart_reference': 'government_spending.spending_by_category',
                    'actionable': True
                })
            
            # Property Market Insights
            if 'property_market' in charts:
                insights.append({
                    'category': 'Property Market',
                    'insight': 'Property market analysis reveals pricing trends and market dynamics',
                    'chart_reference': 'property_market.rental_distribution',
                    'actionable': True
                })
            
        except Exception as e:
            self.logger.error(f"Error generating visual insights: {e}")
        
        return insights
    
    def _generate_chart_recommendations(self, charts: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate recommendations for chart usage and interpretation"""
        recommendations = []
        
        try:
            recommendations.extend([
                {
                    'category': 'Dashboard Usage',
                    'recommendation': 'Use the executive metrics dashboard for quick overview of key performance indicators',
                    'priority': 'High',
                    'implementation': 'Display KPI cards prominently on main dashboard'
                },
                {
                    'category': 'Trend Analysis',
                    'recommendation': 'Monitor economic indicator trends for early warning signals',
                    'priority': 'High',
                    'implementation': 'Set up automated alerts for significant trend changes'
                },
                {
                    'category': 'Risk Management',
                    'recommendation': 'Use cross-sector correlation charts to identify systemic risks',
                    'priority': 'Medium',
                    'implementation': 'Regular review of sector health scorecard'
                },
                {
                    'category': 'Strategic Planning',
                    'recommendation': 'Leverage business formation charts for market opportunity identification',
                    'priority': 'Medium',
                    'implementation': 'Quarterly analysis of industry distribution trends'
                }
            ])
            
        except Exception as e:
            self.logger.error(f"Error generating chart recommendations: {e}")
        
        return recommendations
    
    def save_enhanced_report(self, report: Dict[str, Any], output_path: str = None) -> str:
        """Save enhanced visual report to file"""
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"enhanced_visual_intelligence_report_{timestamp}.json"
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, default=str, ensure_ascii=False)
            
            self.logger.info(f"Enhanced visual report saved to: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Error saving enhanced report: {e}")
            return ""
    
    def close(self):
        """Clean up resources"""
        if hasattr(self, 'data_connector'):
            self.data_connector.close()
        if hasattr(self, 'intelligence_platform'):
            self.intelligence_platform.close()

def main():
    """Main function to demonstrate enhanced visual intelligence platform"""
    try:
        # Initialize platform
        platform = EnhancedVisualIntelligencePlatform()
        
        # Generate enhanced visual report
        print("Generating enhanced visual intelligence report...")
        report = platform.generate_enhanced_visual_report()
        
        if report:
            # Save report
            output_path = platform.save_enhanced_report(report)
            print(f"Enhanced visual report generated and saved to: {output_path}")
            
            # Print summary
            print(f"\nReport Summary:")
            print(f"- Report Type: {report.get('report_type', 'Unknown')}")
            print(f"- Visualizations: {report.get('visualization_count', 0)}")
            print(f"- Visual Insights: {len(report.get('visual_insights', []))}")
            print(f"- Chart Recommendations: {len(report.get('chart_recommendations', []))}")
            
        else:
            print("Failed to generate enhanced visual report")
        
        # Clean up
        platform.close()
        
    except Exception as e:
        print(f"Error in main: {e}")
        logging.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()