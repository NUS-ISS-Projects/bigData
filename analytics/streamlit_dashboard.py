#!/usr/bin/env python3
"""
Streamlit Dashboard for Enhanced Economic Intelligence Platform
Provides interactive web interface for LLM-powered economic analysis
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from datetime import datetime, timedelta
import sys
from pathlib import Path
import logging
from typing import Dict, List, Any

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

# Import our custom modules
from enhanced_economic_intelligence import EnhancedEconomicIntelligencePlatform
from silver_data_connector import SilverLayerConnector, DataSourceConfig
from llm_config import LLMConfig, LLMProvider, get_default_config, create_llm_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="🧠 Economic Intelligence Platform",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
    }
    .insight-card {
        background-color: #e8f4fd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #17a2b8;
        margin: 0.5rem 0;
    }
    .anomaly-card {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #ffc107;
        margin: 0.5rem 0;
    }
    .critical-anomaly-card {
        background-color: #f8d7da;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #dc3545;
        margin: 0.5rem 0;
    }
    .recommendation-card {
        background-color: #d4edda;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #28a745;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_economic_data():
    """Load and cache economic data"""
    try:
        connector = SilverLayerConnector()
        
        data = {
            'acra': connector.load_acra_companies(),
            'economic': connector.load_economic_indicators(),
            'government': connector.load_government_expenditure(),
            'property': connector.load_property_market()
        }
        
        connector.close_connection()
        return data
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

@st.cache_data(ttl=600)  # Cache for 10 minutes
def generate_intelligence_report(llm_provider=None, api_key=None):
    """Generate and cache intelligence report"""
    try:
        # Configure LLM if provided
        llm_config = None
        if llm_provider and api_key:
            llm_config = LLMConfig(
                provider=LLMProvider(llm_provider),
                api_key=api_key,
                model_name="gpt-3.5-turbo" if llm_provider == "openai" else "claude-3-sonnet-20240229"
            )
        
        platform = EnhancedEconomicIntelligencePlatform(llm_config=llm_config)
        report = platform.generate_comprehensive_intelligence_report()
        platform.close()
        
        return report
    except Exception as e:
        st.error(f"Error generating report: {e}")
        return None

def create_business_formation_chart(acra_data):
    """Create enhanced business formation visualization with trends"""
    if acra_data.empty:
        return None
    
    try:
        # Check for industry-related columns
        industry_column = None
        for col in ['primary_ssic_description', 'industry_category', 'industry_sector', 'business_sector']:
            if col in acra_data.columns:
                industry_column = col
                break
        
        # Industry distribution with enhanced insights
        if industry_column:
            industry_counts = acra_data[industry_column].value_counts().head(20)
            total_companies = len(acra_data)
            unique_industries = acra_data[industry_column].nunique()
            
            # Calculate percentages for better context
            industry_pct = (industry_counts / total_companies * 100).round(1)
            
            # Create enhanced labels with percentages
            labels = [f"{idx[:40]}... ({count:,}, {pct}%)" if len(idx) > 40 else f"{idx} ({count:,}, {pct}%)" 
                     for idx, count, pct in zip(industry_counts.index, industry_counts.values, industry_pct)]
            
            fig = px.bar(
                x=industry_counts.values,
                y=labels,
                orientation='h',
                title=f"Singapore Business Formation by {industry_column.replace('_', ' ').title()} (Top 20 of {unique_industries} categories, {total_companies:,} total companies)",
                labels={'x': 'Number of Companies', 'y': industry_column.replace('_', ' ').title()},
                color=industry_counts.values,
                color_continuous_scale='viridis'
            )
            
            fig.update_layout(
                height=700,
                margin=dict(l=300, r=50, t=100, b=50),
                yaxis_tickfont_size=10,
                showlegend=False,
                coloraxis_showscale=False
            )
            
            # Reverse y-axis to show highest values at top
            fig.update_yaxes(autorange="reversed")
            
            return fig
        
        # Alternative: Geographic distribution if no industry data
        elif 'reg_postal_code' in acra_data.columns:
            # Group by postal code regions (first 2 digits)
            acra_data['postal_region'] = acra_data['reg_postal_code'].astype(str).str[:2]
            region_counts = acra_data['postal_region'].value_counts().head(15)
            total_companies = len(acra_data)
            region_pct = (region_counts / total_companies * 100).round(1)
            
            # Create enhanced labels with percentages
            labels = [f"Region {region} ({count:,}, {pct}%)" 
                     for region, count, pct in zip(region_counts.index, region_counts.values, region_pct)]
            
            fig = px.bar(
                x=region_counts.values,
                y=labels,
                orientation='h',
                title=f"Singapore Business Formation by Postal Region (Top 15 regions, {total_companies:,} total companies)",
                labels={'x': 'Number of Companies', 'y': 'Postal Region'},
                color=region_counts.values,
                color_continuous_scale='plasma'
            )
            
            fig.update_layout(
                height=600,
                margin=dict(l=200, r=50, t=100, b=50),
                yaxis_tickfont_size=10,
                showlegend=False,
                coloraxis_showscale=False
            )
            
            # Reverse y-axis to show highest values at top
            fig.update_yaxes(autorange="reversed")
            
            return fig
            
        elif 'entity_type' in acra_data.columns:
            # Enhanced entity type visualization
            entity_counts = acra_data['entity_type'].value_counts().head(12)
            total_companies = len(acra_data)
            entity_pct = (entity_counts / total_companies * 100).round(1)
            
            # Create pie chart for entity types
            fig = px.pie(
                values=entity_counts.values,
                names=[f"{name} ({count:,}, {pct}%)" for name, count, pct in zip(entity_counts.index, entity_counts.values, entity_pct)],
                title=f"Business Entity Distribution ({total_companies:,} companies total)"
            )
            
            fig.update_layout(height=500)
            return fig
            
    except Exception as e:
        st.error(f"Error creating business formation chart: {e}")
    
    return None

def create_economic_indicators_chart(econ_data):
    """Create economic indicators visualization"""
    if econ_data.empty:
        return None
    
    try:
        if 'table_id' in econ_data.columns and 'value_numeric' in econ_data.columns:
            # Get latest values for key indicators using table_id - show top 15 instead of 8
            latest_values = econ_data.groupby('table_id')['value_numeric'].last().head(15)
            
            # Create shorter, more readable names
            def create_short_name(text, max_length=30):
                """Create shorter names by extracting key terms"""
                # Remove common prefixes and suffixes
                text = text.replace('CONSUMER PRICE INDEX', 'CPI')
                text = text.replace('EXPENDITURE ON GROSS DOMESTIC PRODUCT', 'GDP EXPENDITURE')
                text = text.replace('GROSS DOMESTIC PRODUCT', 'GDP')
                text = text.replace('DOMESTIC PRODUCT', 'PRODUCT')
                text = text.replace('CURRENT PRICES', 'CURRENT $')
                text = text.replace('CONSTANT PRICES', 'CONSTANT $')
                text = text.replace('PERCENTAGE CHANGE', '% CHANGE')
                text = text.replace('YEAR ON YEAR', 'YoY')
                text = text.replace('QUARTER ON QUARTER', 'QoQ')
                text = text.replace('SEASONALLY ADJUSTED', 'SA')
                
                # If still too long, truncate intelligently
                if len(text) > max_length:
                    # Try to keep the most important parts
                    words = text.split()
                    if len(words) > 3:
                        # Keep first and last few words
                        text = ' '.join(words[:2]) + '...' + ' '.join(words[-2:])
                    else:
                        text = text[:max_length-3] + '...'
                
                return text
            
            # Use horizontal bar chart for better label readability
            display_names = [create_short_name(name) for name in latest_values.index]
            
            fig = px.bar(
                x=latest_values.values,
                y=display_names,
                orientation='h',  # Horizontal bars
                title=f"Latest Economic Indicators (Top 15 of {econ_data['table_id'].nunique()} total)",
                labels={'x': 'Value', 'y': 'Indicator'}
            )
            
            # Update layout for horizontal bar chart
            fig.update_layout(
                height=700,  # Taller for horizontal layout
                margin=dict(l=250, r=50, t=100, b=50),  # More left margin for labels
                yaxis_tickfont_size=11,
                showlegend=False
            )
            
            # Reverse y-axis to show highest values at top
            fig.update_yaxes(autorange="reversed")
            
            return fig
        elif 'indicator' in econ_data.columns and 'value_original' in econ_data.columns:
            # Fallback to original value if numeric not available
            latest_values = econ_data.groupby('indicator')['value_original'].last().head(8)
            
            fig = px.bar(
                x=latest_values.index,
                y=latest_values.values,
                title="Latest Economic Indicators",
                labels={'x': 'Indicator', 'y': 'Value'}
            )
            fig.update_xaxes(tickangle=45)
            fig.update_layout(height=400)
            return fig
    except Exception as e:
        st.error(f"Error creating economic indicators chart: {e}")
    
    return None

def create_government_spending_chart(gov_data):
    """Create government spending visualization"""
    if gov_data.empty:
        return None
    
    try:
        if 'category' in gov_data.columns and 'amount_million_numeric' in gov_data.columns:
            spending_by_category = gov_data.groupby('category')['amount_million_numeric'].sum().sort_values(ascending=False)
            
            fig = px.pie(
                values=spending_by_category.values,
                names=spending_by_category.index,
                title="Government Expenditure by Category"
            )
            fig.update_layout(height=400)
            return fig
        elif 'expenditure_category' in gov_data.columns and 'amount_sgd_calculated' in gov_data.columns:
            # Alternative column names
            spending_by_category = gov_data.groupby('expenditure_category')['amount_sgd_calculated'].sum().sort_values(ascending=False)
            
            fig = px.pie(
                values=spending_by_category.values,
                names=spending_by_category.index,
                title="Government Expenditure by Category"
            )
            fig.update_layout(height=400)
            return fig
    except Exception as e:
        st.error(f"Error creating government spending chart: {e}")
    
    return None

def create_property_market_chart(prop_data):
    """Create property market visualization"""
    if prop_data.empty:
        return None
    
    try:
        if 'rental_median' in prop_data.columns:
            # Filter out null values
            valid_data = prop_data[prop_data['rental_median'].notna()]
            if not valid_data.empty:
                fig = px.histogram(
                    valid_data,
                    x='rental_median',
                    nbins=20,
                    title="Distribution of Median Rental Prices",
                    labels={'rental_median': 'Median Rental Price (SGD)', 'count': 'Frequency'}
                )
                fig.update_layout(height=400)
                return fig
        elif 'rental_psf25' in prop_data.columns:
            # Fallback to 25th percentile rental PSF
            valid_data = prop_data[prop_data['rental_psf25'].notna()]
            if not valid_data.empty:
                fig = px.histogram(
                    valid_data,
                    x='rental_psf25',
                    nbins=20,
                    title="Distribution of Rental Prices (25th Percentile PSF)",
                    labels={'rental_psf25': 'Rental Price PSF (SGD)', 'count': 'Frequency'}
                )
                fig.update_layout(height=400)
                return fig
    except Exception as e:
        st.error(f"Error creating property market chart: {e}")
    
    return None

def display_insights(insights):
    """Display economic insights"""
    if not insights:
        st.info("No insights available")
        return
    
    for insight in insights:
        with st.expander(f"📊 {insight.get('title', 'Economic Insight')}", expanded=False):
            st.markdown(f"**Type:** {insight.get('insight_type', 'Unknown').title()}")
            st.markdown(f"**Confidence Score:** {insight.get('confidence_score', 0):.2f}")
            st.markdown(f"**Description:** {insight.get('description', 'No description available')}")
            
            # Display LLM analysis
            if insight.get('llm_analysis'):
                st.markdown("**LLM Analysis:**")
                st.markdown(insight['llm_analysis'])
            
            # Display key metrics
            if insight.get('key_metrics'):
                st.markdown("**Key Metrics:**")
                metrics_df = pd.DataFrame(list(insight['key_metrics'].items()), columns=['Metric', 'Value'])
                st.dataframe(metrics_df, use_container_width=True)
            
            # Display recommendations
            if insight.get('recommendations'):
                st.markdown("**Recommendations:**")
                for rec in insight['recommendations']:
                    st.markdown(f"• {rec}")
            
            # Display risk factors
            if insight.get('risk_factors'):
                st.markdown("**Risk Factors:**")
                for risk in insight['risk_factors']:
                    st.markdown(f"⚠️ {risk}")

def display_anomalies(anomalies):
    """Display anomaly alerts"""
    if not anomalies:
        st.info("No anomalies detected")
        return
    
    # Group by severity
    critical_anomalies = [a for a in anomalies if a.get('severity') == 'critical']
    high_anomalies = [a for a in anomalies if a.get('severity') == 'high']
    medium_anomalies = [a for a in anomalies if a.get('severity') == 'medium']
    
    # Display critical anomalies first
    if critical_anomalies:
        st.markdown("### 🚨 Critical Anomalies")
        for anomaly in critical_anomalies:
            with st.expander(f"🚨 {anomaly.get('description', 'Critical Anomaly')}", expanded=True):
                display_anomaly_details(anomaly)
    
    # Display high severity anomalies
    if high_anomalies:
        st.markdown("### ⚠️ High Severity Anomalies")
        for anomaly in high_anomalies:
            with st.expander(f"⚠️ {anomaly.get('description', 'High Severity Anomaly')}", expanded=False):
                display_anomaly_details(anomaly)
    
    # Display medium severity anomalies
    if medium_anomalies:
        st.markdown("### ⚡ Medium Severity Anomalies")
        for anomaly in medium_anomalies:
            with st.expander(f"⚡ {anomaly.get('description', 'Medium Severity Anomaly')}", expanded=False):
                display_anomaly_details(anomaly)

def display_anomaly_details(anomaly):
    """Display detailed anomaly information"""
    st.markdown(f"**Type:** {anomaly.get('alert_type', 'Unknown').title()}")
    st.markdown(f"**Confidence Score:** {anomaly.get('confidence_score', 0):.2f}")
    
    if anomaly.get('llm_explanation'):
        st.markdown("**LLM Explanation:**")
        st.markdown(anomaly['llm_explanation'])
    
    if anomaly.get('potential_causes'):
        st.markdown("**Potential Causes:**")
        for cause in anomaly['potential_causes']:
            st.markdown(f"• {cause}")
    
    if anomaly.get('recommended_actions'):
        st.markdown("**Recommended Actions:**")
        for action in anomaly['recommended_actions']:
            st.markdown(f"✅ {action}")
    
    if anomaly.get('monitoring_requirements'):
        st.markdown("**Monitoring Requirements:**")
        for req in anomaly['monitoring_requirements']:
            st.markdown(f"📊 {req}")

def main():
    """Main Streamlit application"""
    # Header
    st.markdown('<h1 class="main-header">🧠 Enhanced Economic Intelligence Platform</h1>', unsafe_allow_html=True)
    st.markdown("**LLM-Powered Economic Analysis for Singapore**")
    
    # Sidebar configuration
    st.sidebar.header("⚙️ Configuration")
    
    # LLM Configuration
    st.sidebar.subheader("🤖 LLM Settings")
    enable_llm = st.sidebar.checkbox("Enable LLM Analysis", value=False, help="Enable for enhanced insights")
    
    llm_provider = None
    api_key = None
    
    if enable_llm:
        llm_provider = st.sidebar.selectbox(
            "LLM Provider",
            ["openai", "anthropic", "azure_openai", "ollama"],
            help="Select your preferred LLM provider"
        )
        
        api_key = st.sidebar.text_input(
            "API Key",
            type="password",
            help="Enter your API key for the selected provider"
        )
        
        if not api_key:
            st.sidebar.warning("API key required for LLM analysis")
    
    # Data refresh
    st.sidebar.subheader("🔄 Data Management")
    if st.sidebar.button("🔄 Refresh Data", help="Clear cache and reload data"):
        st.cache_data.clear()
        st.rerun()
    
    # Auto-refresh option
    auto_refresh = st.sidebar.checkbox("Auto-refresh (5 min)", value=False)
    if auto_refresh:
        st.sidebar.info("Dashboard will auto-refresh every 5 minutes")
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Dashboard", 
        "🧠 Intelligence Report", 
        "📈 Data Visualization", 
        "🚨 Anomaly Detection", 
        "🤖 LLM Analysis",
        "⚙️ System Status"
    ])
    
    # Load data
    with st.spinner("Loading economic data..."):
        data = load_economic_data()
    
    if data is None:
        st.error("Failed to load data. Please check your data sources.")
        return
    
    # Tab 1: Dashboard Overview
    with tab1:
        st.header("📊 Economic Intelligence Dashboard - Singapore")
        
        # Enhanced Key metrics with delta indicators
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            companies_count = len(data['acra']) if not data['acra'].empty else 0
            active_companies = len(data['acra'][data['acra']['entity_status'] == 'Live']) if not data['acra'].empty and 'entity_status' in data['acra'].columns else companies_count
            st.metric(
                "Total Companies", 
                f"{companies_count:,}",
                delta=f"{active_companies:,} Active" if active_companies != companies_count else None
            )
        
        with col2:
            indicators_count = data['economic']['table_id'].nunique() if not data['economic'].empty and 'table_id' in data['economic'].columns else 0
            latest_period = data['economic']['data_series_id'].nunique() if not data['economic'].empty and 'data_series_id' in data['economic'].columns else 0
            st.metric(
                "Economic Indicators", 
                indicators_count,
                delta=f"{latest_period:,} Data Series"
            )
        
        with col3:
            gov_spending = data['government']['amount_million_numeric'].sum() if not data['government'].empty and 'amount_million_numeric' in data['government'].columns else 0
            avg_spending = gov_spending / len(data['government']) if not data['government'].empty else 0
            st.metric(
                "Gov Spending (M SGD)", 
                f"{gov_spending:.1f}",
                delta=f"Avg: ${avg_spending:.1f}M per category"
            )
        
        with col4:
            property_records = len(data['property']) if not data['property'].empty else 0
            unique_projects = data['property']['project'].nunique() if not data['property'].empty and 'project' in data['property'].columns else 0
            st.metric(
                "Property Records", 
                f"{property_records:,}",
                delta=f"{unique_projects} Unique Projects" if unique_projects > 0 else None
            )
        
        # Enhanced insights with data quality indicators
        st.subheader("🔍 Key Economic Insights")
        
        # Create three columns for better layout
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**🏢 Business Landscape**")
            if not data['acra'].empty and 'primary_ssic_description' in data['acra'].columns:
                top_industry = data['acra']['primary_ssic_description'].value_counts().index[0]
                industry_count = data['acra']['primary_ssic_description'].value_counts().iloc[0]
                industry_pct = (industry_count / len(data['acra'])) * 100
                st.info(f"**Leading Industry:** {top_industry[:35]}... ({industry_count:,} companies, {industry_pct:.1f}%)")
                
                # Show top 3 industries for better insight
                top_3_industries = data['acra']['primary_ssic_description'].value_counts().head(3)
                industry_diversity = len(data['acra']['primary_ssic_description'].unique())
                st.success(f"**Industry Diversity:** {industry_diversity:,} unique sectors represented")
                
            if not data['acra'].empty and 'entity_type' in data['acra'].columns:
                entity_counts = data['acra']['entity_type'].value_counts()
                top_entity = entity_counts.index[0]
                entity_count = entity_counts.iloc[0]
                entity_pct = (entity_count / len(data['acra'])) * 100
                
                # Enhanced entity type display
                total_entities = len(entity_counts)
                st.info(f"**Dominant Entity:** {top_entity} ({entity_count:,} companies, {entity_pct:.1f}%)")
                st.success(f"**Entity Diversity:** {total_entities} different entity types")
            
            # Business formation trends
            if not data['acra'].empty and 'registration_date' in data['acra'].columns:
                recent_registrations = data['acra'][data['acra']['registration_date'] >= '2023-01-01']
                if not recent_registrations.empty:
                    growth_rate = (len(recent_registrations) / len(data['acra'])) * 100
                    st.success(f"**Recent Growth:** {len(recent_registrations):,} companies since 2023 ({growth_rate:.1f}% of total)")
        
        with col2:
            st.markdown("**📊 Economic Health**")
            if not data['economic'].empty and 'table_id' in data['economic'].columns and 'value_numeric' in data['economic'].columns:
                # Look for GDP indicators
                gdp_data = data['economic'][data['economic']['table_id'].str.contains('GDP', case=False, na=False)]
                if not gdp_data.empty:
                    latest_gdp = gdp_data['value_numeric'].iloc[-1]
                    gdp_growth = gdp_data['value_numeric'].pct_change().iloc[-1] * 100 if len(gdp_data) > 1 else 0
                    st.info(f"**GDP Indicator:** {latest_gdp:.2f} ({gdp_growth:+.1f}% change)")
                
                # Economic diversity
                unique_indicators = data['economic']['table_id'].nunique()
                st.success(f"**Economic Diversity:** {unique_indicators} different indicators tracked")
        
        with col3:
            st.markdown("**🏛️ Government & Property**")
            if not data['government'].empty and 'category' in data['government'].columns and 'amount_million_numeric' in data['government'].columns:
                spending_by_category = data['government'].groupby('category')['amount_million_numeric'].sum().sort_values(ascending=False)
                top_spending = spending_by_category.index[0]
                spending_amount = spending_by_category.iloc[0]
                total_spending = data['government']['amount_million_numeric'].sum()
                spending_pct = (spending_amount / total_spending) * 100
                
                # Clean up category name for display
                display_category = top_spending.replace('_', ' ').title() if isinstance(top_spending, str) else str(top_spending)
                st.info(f"**Priority Spending:** {display_category[:30]}... (${spending_amount:.1f}M, {spending_pct:.1f}%)")
                
                # Show spending diversity
                spending_categories = len(spending_by_category)
                st.success(f"**Spending Categories:** {spending_categories} different areas tracked")
            
            if not data['property'].empty:
                # Check for different rental columns (in order of preference)
                rental_col = None
                for col_name in ['median_rental_psf', 'rental_median', 'rental_psf_median', 'rental_psf25', 'rental_price_psf']:
                    if col_name in data['property'].columns:
                        rental_col = col_name
                        break
                
                if rental_col and data['property'][rental_col].notna().any():
                    valid_rentals = data['property'][data['property'][rental_col].notna()]
                    avg_rental = valid_rentals[rental_col].mean()
                    median_rental = valid_rentals[rental_col].median()
                    rental_range = valid_rentals[rental_col].max() - valid_rentals[rental_col].min()
                    
                    # Format rental values properly
                    st.info(f"**Property Market:** ${avg_rental:.2f} avg rental (median: ${median_rental:.2f})")
                    st.success(f"**Market Range:** ${rental_range:.2f} SGD spread across {len(valid_rentals):,} properties")
                else:
                    # Fallback if no rental data - show available columns for debugging
                    property_count = len(data['property'])
                    available_cols = list(data['property'].columns)
                    
                    # Try to find project-related columns
                    project_col = None
                    for col_name in ['project', 'project_name', 'development', 'property_name']:
                        if col_name in data['property'].columns:
                            project_col = col_name
                            break
                    
                    st.info(f"**Property Records:** {property_count:,} total records")
                    if project_col:
                        unique_projects = data['property'][project_col].nunique()
                        st.success(f"**Project Diversity:** {unique_projects} unique developments")
                    else:
                        # Show property types if available
                        if 'property_type' in data['property'].columns:
                            unique_types = data['property']['property_type'].nunique()
                            st.success(f"**Property Types:** {unique_types} different types tracked")
        
        # Data freshness indicator
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.caption(f"📅 Data loaded: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        with col2:
            total_records = sum(len(df) for df in data.values())
            st.caption(f"📊 Total records: {total_records:,}")
        with col3:
            data_sources = sum(1 for df in data.values() if not df.empty)
            st.caption(f"🔗 Active sources: {data_sources}/4")
    
    # Tab 2: Intelligence Report
    with tab2:
        st.header("🧠 Comprehensive Intelligence Report")
        
        if st.button("🚀 Generate Intelligence Report", type="primary"):
            with st.spinner("Generating comprehensive intelligence report..."):
                report = generate_intelligence_report(llm_provider, api_key)
            
            if report:
                # Executive Summary
                if report.get('executive_summary'):
                    st.subheader("📋 Executive Summary")
                    st.markdown(report['executive_summary'])
                
                # Strategic Recommendations
                if report.get('strategic_recommendations'):
                    st.subheader("🎯 Strategic Recommendations")
                    for i, rec in enumerate(report['strategic_recommendations'], 1):
                        st.markdown(f"{i}. {rec}")
                
                # Risk Assessment
                if report.get('risk_assessment'):
                    st.subheader("⚠️ Risk Assessment")
                    for i, risk in enumerate(report['risk_assessment'], 1):
                        st.markdown(f"{i}. {risk}")
                
                # Detailed Insights
                if report.get('insights'):
                    st.subheader("📊 Detailed Economic Insights")
                    display_insights(report['insights'])
                
                # Download report
                report_json = json.dumps(report, indent=2, default=str)
                st.download_button(
                    label="📥 Download Full Report (JSON)",
                    data=report_json,
                    file_name=f"economic_intelligence_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
            else:
                st.error("Failed to generate intelligence report")
        
        st.info("💡 **Tip:** Enable LLM analysis in the sidebar for enhanced insights and explanations.")
    
    # Tab 3: Data Visualization
    with tab3:
        st.header("📈 Economic Data Visualization")
        
        # Business Formation Chart
        st.subheader("🏢 Business Formation Analysis")
        business_chart = create_business_formation_chart(data['acra'])
        if business_chart:
            st.plotly_chart(business_chart, use_container_width=True)
        else:
            st.info("No business formation data available for visualization")
        
        # Economic Indicators Chart
        st.subheader("📊 Economic Indicators")
        econ_chart = create_economic_indicators_chart(data['economic'])
        if econ_chart:
            st.plotly_chart(econ_chart, use_container_width=True)
        else:
            st.info("No economic indicators data available for visualization")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Government Spending Chart
            st.subheader("🏛️ Government Expenditure")
            gov_chart = create_government_spending_chart(data['government'])
            if gov_chart:
                st.plotly_chart(gov_chart, use_container_width=True)
            else:
                st.info("No government spending data available")
        
        with col2:
            # Property Market Chart
            st.subheader("🏠 Property Market")
            prop_chart = create_property_market_chart(data['property'])
            if prop_chart:
                st.plotly_chart(prop_chart, use_container_width=True)
            else:
                st.info("No property market data available")
    
    # Tab 4: Anomaly Detection
    with tab4:
        st.header("🚨 Anomaly Detection & Alerts")
        
        if st.button("🔍 Run Anomaly Detection", type="primary"):
            with st.spinner("Detecting anomalies across all data sources..."):
                try:
                    platform = EnhancedEconomicIntelligencePlatform()
                    anomalies = platform.anomaly_detector.detect_comprehensive_anomalies()
                    platform.close()
                    
                    if anomalies:
                        # Summary metrics
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            total_anomalies = len(anomalies)
                            st.metric("Total Anomalies", total_anomalies)
                        
                        with col2:
                            critical_count = len([a for a in anomalies if a.severity == 'critical'])
                            st.metric("Critical Alerts", critical_count)
                        
                        with col3:
                            high_count = len([a for a in anomalies if a.severity == 'high'])
                            st.metric("High Priority", high_count)
                        
                        # Convert to dict format for display
                        anomalies_dict = [{
                            'alert_type': a.alert_type,
                            'severity': a.severity,
                            'description': a.description,
                            'llm_explanation': a.llm_explanation,
                            'confidence_score': a.confidence_score,
                            'potential_causes': a.potential_causes,
                            'recommended_actions': a.recommended_actions,
                            'monitoring_requirements': a.monitoring_requirements
                        } for a in anomalies]
                        
                        display_anomalies(anomalies_dict)
                    else:
                        st.success("✅ No anomalies detected. All systems operating normally.")
                
                except Exception as e:
                    st.error(f"Error running anomaly detection: {e}")
        
        st.info("💡 **Note:** Anomaly detection analyzes patterns across business formation, economic indicators, government spending, and property market data.")
    
    # Tab 5: LLM Analysis
    with tab5:
        st.header("🤖 LLM-Powered Economic Analysis (Llama 8B)")
        
        # LLM Configuration Status
        st.subheader("🔧 LLM Configuration")
        
        try:
            # Use local Ollama configuration
            llm_config = get_default_config()
            llm_client = create_llm_client(llm_config)
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Provider", "Ollama (Local)")
            with col2:
                st.metric("Model", "Llama 8B")
            with col3:
                st.metric("Max Tokens", "4,000")
            with col4:
                st.metric("Status", "✅ Ready")
                
            st.success("🦙 Llama 8B model is configured and ready for comprehensive economic analysis!")
            
            # Enhanced Analysis Options
            st.subheader("📊 Advanced Analysis Options")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                analysis_type = st.selectbox(
                    "Select Analysis Type:",
                    [
                        "Comprehensive Economic Overview",
                        "Business Formation Deep Dive",
                        "Government Spending Impact Analysis",
                        "Property Market Intelligence",
                        "Cross-Sector Correlation Analysis",
                        "Economic Risk Assessment",
                        "Investment Opportunity Analysis",
                        "Policy Impact Evaluation",
                        "Custom Economic Query"
                    ]
                )
            
            with col2:
                analysis_depth = st.selectbox(
                    "Analysis Depth:",
                    ["Standard", "Detailed", "Comprehensive"]
                )
            
            # Enhanced context options
            st.subheader("🎯 Analysis Context")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                include_trends = st.checkbox("Include Trend Analysis", value=True)
                include_comparisons = st.checkbox("Include Sector Comparisons", value=True)
            
            with col2:
                include_predictions = st.checkbox("Include Future Outlook", value=True)
                include_risks = st.checkbox("Include Risk Assessment", value=True)
            
            with col3:
                include_recommendations = st.checkbox("Include Recommendations", value=True)
                include_metrics = st.checkbox("Include Key Metrics", value=True)
            
            # Custom query input for flexible analysis
            if analysis_type == "Custom Economic Query":
                custom_query = st.text_area(
                    "Enter your economic analysis question:",
                    placeholder="e.g., What are the emerging opportunities in Singapore's fintech sector based on recent business formations and government initiatives?",
                    height=100
                )
            
            # Generate Analysis Button
            if st.button("🚀 Generate Enhanced LLM Analysis", type="primary"):
                with st.spinner("Generating comprehensive AI-powered analysis using Llama 8B..."):
                    try:
                        # Prepare enhanced context based on loaded data
                        context_data = {
                            "companies_count": len(data['acra']) if not data['acra'].empty else 0,
                            "economic_indicators": data['economic']['table_id'].nunique() if not data['economic'].empty and 'table_id' in data['economic'].columns else 0,
                            "gov_spending_total": data['government']['amount_million_numeric'].sum() if not data['government'].empty and 'amount_million_numeric' in data['government'].columns else 0,
                            "property_records": len(data['property']) if not data['property'].empty else 0
                        }
                        
                        # Enhanced data insights for context
                        enhanced_context = ""
                        if not data['acra'].empty:
                            top_industries = data['acra']['primary_ssic_description'].value_counts().head(5).to_dict() if 'primary_ssic_description' in data['acra'].columns else {}
                            enhanced_context += f"Top Industries: {', '.join([f'{k} ({v})' for k, v in list(top_industries.items())[:3]])}. "
                        
                        if not data['economic'].empty and 'table_id' in data['economic'].columns:
                            top_indicators = data['economic']['table_id'].value_counts().head(3).index.tolist()
                            enhanced_context += f"Key Economic Indicators: {', '.join(top_indicators)}. "
                        
                        if not data['government'].empty and 'ministry' in data['government'].columns:
                            top_ministries = data['government']['ministry'].value_counts().head(3).index.tolist()
                            enhanced_context += f"Major Government Spenders: {', '.join(top_ministries)}. "
                        
                        # Create comprehensive analysis prompt based on selection
                        depth_modifier = {
                            "Standard": "Provide a clear and concise analysis",
                            "Detailed": "Provide an in-depth analysis with detailed explanations",
                            "Comprehensive": "Provide a comprehensive analysis with extensive insights, correlations, and implications"
                        }[analysis_depth]
                        
                        context_options = []
                        if include_trends: context_options.append("trend analysis")
                        if include_comparisons: context_options.append("sector comparisons")
                        if include_predictions: context_options.append("future outlook")
                        if include_risks: context_options.append("risk assessment")
                        if include_recommendations: context_options.append("actionable recommendations")
                        if include_metrics: context_options.append("key performance metrics")
                        
                        context_instruction = f"Include {', '.join(context_options)} in your analysis." if context_options else ""
                        
                        if analysis_type == "Comprehensive Economic Overview":
                            prompt = f"""{depth_modifier} of Singapore's overall economic landscape based on:
                            - {context_data['companies_count']:,} registered companies
                            - {context_data['economic_indicators']} economic indicators tracked
                            - ${context_data['gov_spending_total']:.1f}M SGD in government spending
                            - {context_data['property_records']:,} property market records
                            
                            Context: {enhanced_context}
                            
                            {context_instruction}
                            
                            Focus on: Economic health, growth drivers, structural changes, and strategic opportunities."""
                            
                        elif analysis_type == "Business Formation Deep Dive":
                            prompt = f"""{depth_modifier} of Singapore's business formation landscape with {context_data['companies_count']:,} companies.
                            
                            Context: {enhanced_context}
                            
                            {context_instruction}
                            
                            Analyze: Formation patterns, industry dynamics, entrepreneurial ecosystem, regulatory impact, and growth potential."""
                            
                        elif analysis_type == "Government Spending Impact Analysis":
                            prompt = f"""{depth_modifier} of Singapore's government spending impact with ${context_data['gov_spending_total']:.1f}M SGD total expenditure.
                            
                            Context: {enhanced_context}
                            
                            {context_instruction}
                            
                            Evaluate: Spending efficiency, economic multiplier effects, policy priorities, and fiscal sustainability."""
                            
                        elif analysis_type == "Property Market Intelligence":
                            prompt = f"""{depth_modifier} of Singapore's property market with {context_data['property_records']:,} records.
                            
                            Context: {enhanced_context}
                            
                            {context_instruction}
                            
                            Examine: Market dynamics, price trends, investment flows, policy impacts, and economic indicators."""
                            
                        elif analysis_type == "Cross-Sector Correlation Analysis":
                            prompt = f"""{depth_modifier} of cross-sector correlations in Singapore's economy.
                            
                            Data Sources: {context_data['companies_count']:,} companies, {context_data['economic_indicators']} indicators, ${context_data['gov_spending_total']:.1f}M gov spending, {context_data['property_records']:,} property records.
                            
                            Context: {enhanced_context}
                            
                            {context_instruction}
                            
                            Identify: Inter-sector dependencies, spillover effects, systemic risks, and synergistic opportunities."""
                            
                        elif analysis_type == "Economic Risk Assessment":
                            prompt = f"""{depth_modifier} of economic risks in Singapore based on comprehensive data analysis.
                            
                            Context: {enhanced_context}
                            
                            {context_instruction}
                            
                            Assess: Systemic risks, sector vulnerabilities, external dependencies, and mitigation strategies."""
                            
                        elif analysis_type == "Investment Opportunity Analysis":
                            prompt = f"""{depth_modifier} of investment opportunities in Singapore's economy.
                            
                            Context: {enhanced_context}
                            
                            {context_instruction}
                            
                            Identify: Emerging sectors, growth catalysts, market gaps, and strategic investment themes."""
                            
                        elif analysis_type == "Policy Impact Evaluation":
                            prompt = f"""{depth_modifier} of policy impacts on Singapore's economic landscape.
                            
                            Context: {enhanced_context}
                            
                            {context_instruction}
                            
                            Evaluate: Policy effectiveness, unintended consequences, implementation gaps, and optimization opportunities."""
                            
                        else:  # Custom query
                            prompt = f"""{depth_modifier} based on Singapore's comprehensive economic data:
                            - Companies: {context_data['companies_count']:,}
                            - Economic indicators: {context_data['economic_indicators']}
                            - Government spending: ${context_data['gov_spending_total']:.1f}M SGD
                            - Property records: {context_data['property_records']:,}
                            
                            Context: {enhanced_context}
                            
                            {context_instruction}
                            
                            Query: {custom_query if 'custom_query' in locals() and custom_query else 'Provide a comprehensive economic analysis.'}"""
                        
                        # Generate analysis using LLM
                        analysis_result = llm_client.generate_analysis(
                            prompt=prompt,
                            analysis_type="economic_analysis"
                        )
                        
                        # Display enhanced results
                        st.subheader("🎯 Llama 8B Analysis Results")
                        
                        if analysis_result and 'analysis' in analysis_result:
                            st.markdown(f"""
                            <div class="insight-card">
                                <h4>📈 {analysis_type} ({analysis_depth})</h4>
                                <p>{analysis_result['analysis']}</p>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Show additional insights if available
                            if 'insights' in analysis_result:
                                st.subheader("💡 Key Insights")
                                for i, insight in enumerate(analysis_result['insights'], 1):
                                    st.markdown(f"**{i}.** {insight}")
                            
                            # Show recommendations if available
                            if 'recommendations' in analysis_result:
                                st.subheader("🎯 Recommendations")
                                for i, rec in enumerate(analysis_result['recommendations'], 1):
                                    st.markdown(f"""
                                    <div class="recommendation-card">
                                        <strong>{i}.</strong> {rec}
                                    </div>
                                    """, unsafe_allow_html=True)
                        else:
                            st.warning("Analysis completed but no detailed results available.")
                            
                    except Exception as e:
                        st.error(f"Error generating LLM analysis: {e}")
                        st.info("Please ensure Ollama is running and the model is available.")
            
            # Data Summary Section
            st.subheader("📊 Silver Data Summary")
            
            if data:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**🏢 Business Data:**")
                    if not data['acra'].empty:
                        st.markdown(f"• {len(data['acra']):,} registered companies")
                        if 'primary_ssic_description' in data['acra'].columns:
                            top_industry = data['acra']['primary_ssic_description'].value_counts().index[0]
                            st.markdown(f"• Top industry: {top_industry}")
                        if 'entity_type' in data['acra'].columns:
                            entity_types = data['acra']['entity_type'].nunique()
                            st.markdown(f"• {entity_types} entity types tracked")
                    
                    st.markdown("**🏛️ Government Data:**")
                    if not data['government'].empty:
                        st.markdown(f"• {len(data['government']):,} spending records")
                        if 'amount_million_numeric' in data['government'].columns:
                            total_spending = data['government']['amount_million_numeric'].sum()
                            st.markdown(f"• Total: ${total_spending:.1f}M SGD")
                
                with col2:
                    st.markdown("**📈 Economic Data:**")
                    if not data['economic'].empty:
                        st.markdown(f"• {len(data['economic']):,} economic records")
                        if 'table_id' in data['economic'].columns:
                            indicators = data['economic']['table_id'].nunique()
                            st.markdown(f"• {indicators} unique indicators")
                    
                    st.markdown("**🏠 Property Data:**")
                    if not data['property'].empty:
                        st.markdown(f"• {len(data['property']):,} property records")
                        if 'rental_median' in data['property'].columns:
                            avg_rental = data['property']['rental_median'].mean()
                            st.markdown(f"• Avg rental: ${avg_rental:.0f} SGD")
            
            # Enhanced Sample Queries Section
            st.subheader("💭 Advanced Analysis Examples")
            
            query_categories = {
                "🚀 Innovation & Growth": [
                    "What are the emerging fintech trends based on recent business formations and government digital initiatives?",
                    "How do startup formation patterns correlate with government R&D spending across different sectors?"
                ],
                "🏗️ Infrastructure & Development": [
                    "What is the relationship between government infrastructure spending and property market dynamics?",
                    "How do urban development policies impact business formation in different districts?"
                ],
                "📊 Economic Intelligence": [
                    "What early warning signals can we identify from cross-sector data analysis?",
                    "How do economic indicators predict future business formation trends?"
                ],
                "🎯 Policy Impact": [
                    "What is the effectiveness of government spending on economic diversification efforts?",
                    "How do regulatory changes impact different industry sectors' growth patterns?"
                ]
            }
            
            for category, queries in query_categories.items():
                st.markdown(f"**{category}**")
                for query in queries:
                    st.markdown(f"• {query}")
                st.markdown("")
                
        except Exception as e:
            st.error(f"Error initializing LLM: {e}")
            st.info("Please ensure Ollama is installed and running with the Llama 8B model.")
    
    # Tab 6: System Status
    with tab6:
        st.header("⚙️ System Status & Configuration")
        
        # System Health Check
        st.subheader("🔍 System Health Check")
        
        try:
            connector = SilverLayerConnector()
            db_status = connector.is_connected()
            connector.close_connection()
        except:
            db_status = False
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Database Connection", 
                "✅ Connected" if db_status else "❌ Disconnected",
                delta="Healthy" if db_status else "Check Configuration"
            )
        
        with col2:
            llm_status = enable_llm and api_key
            st.metric(
                "LLM Integration", 
                "✅ Enabled" if llm_status else "❌ Disabled",
                delta="Enhanced Analysis" if llm_status else "Basic Analysis Only"
            )
        
        with col3:
            data_status = data is not None and any(not df.empty for df in data.values())
            st.metric(
                "Data Availability", 
                "✅ Available" if data_status else "❌ Unavailable",
                delta="Ready for Analysis" if data_status else "Check Data Sources"
            )
        
        # Data Source Details
        st.subheader("📊 Data Source Status")
        
        if data:
            data_status_df = pd.DataFrame([
                {"Data Source": "ACRA Companies", "Records": len(data['acra']), "Status": "✅ Available" if not data['acra'].empty else "❌ Empty"},
                {"Data Source": "Economic Indicators", "Records": len(data['economic']), "Status": "✅ Available" if not data['economic'].empty else "❌ Empty"},
                {"Data Source": "Government Expenditure", "Records": len(data['government']), "Status": "✅ Available" if not data['government'].empty else "❌ Empty"},
                {"Data Source": "Property Market", "Records": len(data['property']), "Status": "✅ Available" if not data['property'].empty else "❌ Empty"}
            ])
            
            st.dataframe(data_status_df, use_container_width=True)
        
        # Configuration Details
        st.subheader("⚙️ Current Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🤖 LLM Configuration:**")
            config_info_llm = {
                "Provider": "Ollama (Local)",
                "Model": "Llama 8B (llama3.1:8b)",
                "Max Tokens": "4,000",
                "Temperature": "0.1",
                "Status": "✅ Active" if llm_status else "❌ Inactive"
            }
            
            for key, value in config_info_llm.items():
                st.text(f"{key}: {value}")
        
        with col2:
            st.markdown("**⚙️ System Configuration:**")
            config_info_system = {
                "Auto Refresh": "Enabled" if auto_refresh else "Disabled",
                "Cache TTL": "5 minutes (data)",
                "Dashboard Version": "2.0.0 (Enhanced)",
                "Analysis Depth": "Multi-level Support",
                "Last Updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            for key, value in config_info_system.items():
                st.text(f"{key}: {value}")
        
        # Performance Metrics
        st.subheader("📈 Performance Metrics")
        
        if data:
            total_records = sum(len(df) for df in data.values())
            st.metric("Total Records Processed", f"{total_records:,}")
            
            # Data freshness
            st.text(f"Data Last Loaded: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            st.text(f"Cache Status: Active (TTL: 5 minutes)")
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666;'>" +
        "🧠 Enhanced Economic Intelligence Platform v2.0 | " +
        "🦙 Powered by Llama 8B | " +
        f"📊 {sum(len(df) for df in data.values()) if data else 0:,} Records Analyzed | " +
        f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}" +
        "</div>", 
        unsafe_allow_html=True
    )
    
    # Auto-refresh logic
    if auto_refresh:
        import time
        time.sleep(300)  # 5 minutes
        st.rerun()

if __name__ == "__main__":
    main()