#!/usr/bin/env python3
"""
Example usage of query_data_lake.py for custom analyses
Demonstrates advanced querying capabilities for ACRA, SingStat, and URA datasets
"""

from query_data_lake import DeltaLakeQueryTool
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

def analyze_company_distribution_by_postal_code(df):
    """Analyze company distribution by postal code"""
    print("\n📊 Company Distribution by Postal Code")
    print("=" * 50)
    
    if 'reg_postal_code' not in df.columns:
        print("❌ No postal code data available")
        return None
    
    # Get top 20 postal codes
    postal_distribution = df['reg_postal_code'].value_counts().head(20)
    
    print("Top 20 Postal Codes:")
    for postal_code, count in postal_distribution.items():
        print(f"  {postal_code}: {count:,} companies")
    
    return postal_distribution

def find_recently_registered_companies(df):
    """Find companies registered in the last 30 days"""
    print("\n📅 Recently Registered Companies")
    print("=" * 50)
    
    if 'uen_issue_date' not in df.columns:
        print("❌ No UEN issue date data available")
        return None
    
    # Convert to datetime
    df['uen_issue_date'] = pd.to_datetime(df['uen_issue_date'], errors='coerce')
    
    # Filter last 30 days
    cutoff_date = datetime.now() - pd.Timedelta(days=30)
    recent_companies = df[df['uen_issue_date'] >= cutoff_date]
    
    if recent_companies.empty:
        print("No companies registered in the last 30 days")
        return None
    
    print(f"Found {len(recent_companies)} recently registered companies:")
    
    # Show sample
    sample_cols = ['entity_name', 'uen', 'entity_type', 'uen_issue_date']
    available_cols = [col for col in sample_cols if col in recent_companies.columns]
    
    if available_cols:
        print(recent_companies[available_cols].head(10).to_string(index=False))
    
    return recent_companies

def analyze_entity_types(df):
    """Analyze distribution of entity types"""
    print("\n🏢 Entity Type Analysis")
    print("=" * 50)
    
    if 'entity_type' not in df.columns:
        print("❌ No entity type data available")
        return None
    
    entity_distribution = df['entity_type'].value_counts()
    
    print("Entity Type Distribution:")
    for entity_type, count in entity_distribution.items():
        percentage = (count / len(df)) * 100
        print(f"  {entity_type}: {count:,} ({percentage:.1f}%)")
    
    return entity_distribution

def find_companies_by_street(df, street_name):
    """Find companies on a specific street"""
    print(f"\n🏠 Companies on {street_name}")
    print("=" * 50)
    
    if 'reg_street_name' not in df.columns:
        print("❌ No street name data available")
        return None
    
    # Case-insensitive search
    street_companies = df[df['reg_street_name'].str.contains(street_name, case=False, na=False)]
    
    if street_companies.empty:
        print(f"No companies found on {street_name}")
        return None
    
    print(f"Found {len(street_companies)} companies:")
    
    # Show sample
    sample_cols = ['entity_name', 'uen', 'reg_street_name', 'reg_postal_code']
    available_cols = [col for col in sample_cols if col in street_companies.columns]
    
    if available_cols:
        print(street_companies[available_cols].head(10).to_string(index=False))
    
    return street_companies

# SingStat-specific analysis functions
def analyze_economic_trends(df):
    """Analyze economic trends from SingStat data"""
    print("\n📈 Economic Trends Analysis")
    print("=" * 50)
    
    if 'level_1' not in df.columns or 'value' not in df.columns:
        print("❌ Required columns (level_1, value) not available")
        return None
    
    # Convert value to numeric
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    
    # Analyze by economic categories
    category_stats = df.groupby('level_1')['value'].agg([
        'count', 'mean', 'median', 'std'
    ]).round(2)
    
    print("Economic Categories Statistics:")
    print(category_stats.head(10))
    
    return category_stats

def analyze_gdp_data(df):
    """Analyze GDP-related data from SingStat"""
    print("\n💰 GDP Data Analysis")
    print("=" * 50)
    
    # Filter for GDP-related data
    gdp_mask = df['data_series_title'].str.contains('GDP|Gross Domestic Product', case=False, na=False)
    gdp_data = df[gdp_mask].copy()
    
    if gdp_data.empty:
        print("❌ No GDP data found")
        return None
    
    print(f"Found {len(gdp_data)} GDP-related records")
    
    # Analyze GDP by period
    if 'period' in gdp_data.columns and 'value' in gdp_data.columns:
        gdp_data['value'] = pd.to_numeric(gdp_data['value'], errors='coerce')
        gdp_data = gdp_data.dropna(subset=['value'])
        
        # Group by period
        gdp_by_period = gdp_data.groupby('period')['value'].mean().sort_index()
        
        print("\nGDP by Period (latest 10):")
        print(gdp_by_period.tail(10))
        
        return gdp_by_period
    
    return gdp_data

def analyze_inflation_data(df):
    """Analyze inflation-related data from SingStat"""
    print("\n📊 Inflation Data Analysis")
    print("=" * 50)
    
    # Filter for inflation/CPI data
    inflation_keywords = ['CPI', 'Consumer Price', 'Inflation', 'Price Index']
    inflation_mask = df['data_series_title'].str.contains('|'.join(inflation_keywords), case=False, na=False)
    inflation_data = df[inflation_mask].copy()
    
    if inflation_data.empty:
        print("❌ No inflation data found")
        return None
    
    print(f"Found {len(inflation_data)} inflation-related records")
    
    # Analyze by data series
    if 'data_series_title' in inflation_data.columns:
        series_counts = inflation_data['data_series_title'].value_counts().head(10)
        print("\nTop Inflation Data Series:")
        for series, count in series_counts.items():
            print(f"  {series}: {count} records")
    
    return inflation_data

def analyze_employment_data(df):
    """Analyze employment-related data from SingStat"""
    print("\n👥 Employment Data Analysis")
    print("=" * 50)
    
    # Filter for employment data
    employment_keywords = ['Employment', 'Unemployment', 'Labour', 'Workforce', 'Jobs']
    employment_mask = df['data_series_title'].str.contains('|'.join(employment_keywords), case=False, na=False)
    employment_data = df[employment_mask].copy()
    
    if employment_data.empty:
        print("❌ No employment data found")
        return None
    
    print(f"Found {len(employment_data)} employment-related records")
    
    # Analyze employment trends
    if 'period' in employment_data.columns and 'value' in employment_data.columns:
        employment_data['value'] = pd.to_numeric(employment_data['value'], errors='coerce')
        employment_data = employment_data.dropna(subset=['value'])
        
        # Recent employment statistics
        recent_employment = employment_data.sort_values('period').tail(20)
        
        print("\nRecent Employment Statistics:")
        if not recent_employment.empty:
            print(recent_employment[['period', 'data_series_title', 'value']].to_string(index=False))
    
    return employment_data

# URA-specific analysis functions
def analyze_property_prices(df):
    """Analyze property price data from URA"""
    print("\n🏠 Property Price Analysis")
    print("=" * 50)
    
    if 'price' not in df.columns:
        print("❌ No price data available")
        return None
    
    # Convert price to numeric
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df = df.dropna(subset=['price'])
    
    if df.empty:
        print("❌ No valid price data found")
        return None
    
    # Price statistics
    price_stats = {
        'mean': df['price'].mean(),
        'median': df['price'].median(),
        'min': df['price'].min(),
        'max': df['price'].max(),
        'std': df['price'].std()
    }
    
    print("Property Price Statistics:")
    for stat, value in price_stats.items():
        print(f"  {stat.capitalize()}: ${value:,.2f}")
    
    # Analyze by property type if available
    if 'type_of_area' in df.columns:
        price_by_type = df.groupby('type_of_area')['price'].agg(['mean', 'median', 'count']).round(2)
        print("\nPrice by Property Type:")
        print(price_by_type)
    
    return price_stats

def main():
    """Run example analyses for all datasets"""
    print("🚀 Delta Lake Multi-Dataset Query Examples")
    print("=" * 60)
    
    # ACRA Dataset Examples
    print("\n🏢 ACRA Dataset Analysis")
    print("=" * 40)
    
    acra_tool = DeltaLakeQueryTool(dataset='acra')
    
    # Example 1: Basic ACRA data summary
    print("\n1️⃣ Getting ACRA data summary...")
    acra_summary = acra_tool.get_summary()
    print(f"   Sample size: {acra_summary.get('total_rows_sampled', 0)} rows")
    print(f"   Columns: {len(acra_summary.get('columns', []))}")
    if 'entity_types' in acra_summary:
        print(f"   Top entity types: {list(acra_summary['entity_types'].keys())[:3]}")
    
    # Example 2: Search for ACRA companies
    print("\n2️⃣ Searching for companies with 'TECH' in name...")
    tech_companies = acra_tool.search_entities('TECH')
    if not tech_companies.empty:
        print(f"   Found {len(tech_companies)} tech companies")
    
    # Example 3: ACRA custom analyses
    print("\n3️⃣ Running ACRA custom analyses...")
    acra_df = acra_tool.load_data()  # Load all data
    if not acra_df.empty:
        acra_tool.run_custom_query(analyze_entity_types)
        acra_tool.run_custom_query(analyze_company_distribution_by_postal_code)
    
    # SingStat Dataset Examples
    print("\n\n📊 SingStat Dataset Analysis")
    print("=" * 40)
    
    singstat_tool = DeltaLakeQueryTool(dataset='singstat')
    
    # Example 4: SingStat data summary
    print("\n4️⃣ Getting SingStat data summary...")
    singstat_summary = singstat_tool.get_summary()
    print(f"   Sample size: {singstat_summary.get('total_rows_sampled', 0)} rows")
    print(f"   Columns: {len(singstat_summary.get('columns', []))}")
    if 'top_categories' in singstat_summary:
        print(f"   Top categories: {list(singstat_summary['top_categories'].keys())[:3]}")
    
    # Example 5: Economic indicators analysis
    print("\n5️⃣ Analyzing economic indicators...")
    econ_analysis = singstat_tool.analyze_economic_indicators()
    if econ_analysis and 'categories' in econ_analysis:
        print(f"   Found {len(econ_analysis['categories'])} economic categories")
    
    # Example 6: Search for GDP data
    print("\n6️⃣ Searching for GDP data...")
    gdp_data = singstat_tool.search_entities('GDP')
    if not gdp_data.empty:
        print(f"   Found {len(gdp_data)} GDP-related records")
    
    # Example 7: Time series analysis
    print("\n7️⃣ Getting GDP time series...")
    gdp_series = singstat_tool.get_time_series_data('Gross Domestic Product')
    if not gdp_series.empty:
        print(f"   GDP time series: {len(gdp_series)} records")
    
    # Example 8: SingStat custom analyses
    print("\n8️⃣ Running SingStat custom analyses...")
    singstat_df = singstat_tool.load_data()  # Load all data
    if not singstat_df.empty:
        singstat_tool.run_custom_query(analyze_economic_trends)
        singstat_tool.run_custom_query(analyze_gdp_data)
        singstat_tool.run_custom_query(analyze_inflation_data)
        singstat_tool.run_custom_query(analyze_employment_data)
    
    # URA Dataset Examples
    print("\n\n🏘️ URA Dataset Analysis")
    print("=" * 40)
    
    ura_tool = DeltaLakeQueryTool(dataset='ura')
    
    # Example 9: URA data summary
    print("\n9️⃣ Getting URA data summary...")
    ura_summary = ura_tool.get_summary()
    print(f"   Sample size: {ura_summary.get('total_rows_sampled', 0)} rows")
    print(f"   Columns: {len(ura_summary.get('columns', []))}")
    if 'area_types' in ura_summary:
        print(f"   Area types: {list(ura_summary['area_types'].keys())[:3]}")
    
    # Example 10: Search for property data
    print("\n🔟 Searching for condo data...")
    condo_data = ura_tool.search_entities('Condo')
    if not condo_data.empty:
        print(f"   Found {len(condo_data)} condo-related records")
    
    # Example 11: URA custom analyses
    print("\n1️⃣1️⃣ Running URA custom analyses...")
    ura_df = ura_tool.load_data()  # Load all data
    if not ura_df.empty:
        ura_tool.run_custom_query(analyze_property_prices)
    
    print("\n\n✨ Multi-Dataset Examples Completed!")
    print("\n💡 Usage Tips:")
    print("   - ACRA: python query_data_lake.py --dataset acra summary")
    print("   - SingStat: python query_data_lake.py --dataset singstat search --search-term 'GDP'")
    print("   - URA: python query_data_lake.py --dataset ura summary")
    print("   - Demo: python query_data_lake.py demo")
    print("   - Export: python query_data_lake.py --dataset singstat export --output singstat_data.csv")
    
    print("\n🔧 Available Datasets:")
    print("   - acra: Singapore company registration data")
    print("   - singstat: Economic and statistical indicators")
    print("   - ura: Urban planning and property data")

if __name__ == "__main__":
    main()