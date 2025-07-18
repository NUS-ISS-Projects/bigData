#!/usr/bin/env python3
"""
Example usage of query_data_lake.py for custom analyses
Demonstrates advanced querying capabilities
"""

from query_data_lake import DeltaLakeQueryTool
import pandas as pd
from datetime import datetime

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

def main():
    """Run example analyses"""
    print("🚀 Delta Lake Custom Query Examples")
    print("=" * 60)
    
    # Initialize query tool
    query_tool = DeltaLakeQueryTool()
    
    # Example 1: Basic data summary
    print("\n1️⃣ Getting data summary...")
    summary = query_tool.get_summary()
    print(f"   Sample size: {summary.get('total_rows_sampled', 0)} rows")
    print(f"   Columns: {len(summary.get('columns', []))}")
    
    # Example 2: Search for specific company
    print("\n2️⃣ Searching for companies with 'TECH' in name...")
    tech_companies = query_tool.search_entities('TECH', 'entity_name')
    if not tech_companies.empty:
        print(f"   Found {len(tech_companies)} tech companies")
    
    # Example 3: Filter by entity type
    print("\n3️⃣ Filtering for private limited companies...")
    pte_ltd_companies = query_tool.filter_data({'entity_type': 'PRIVATE LIMITED COMPANY'})
    if not pte_ltd_companies.empty:
        print(f"   Found {len(pte_ltd_companies)} private limited companies")
    
    # Example 4: Get recent data
    print("\n4️⃣ Getting recent data (last 24 hours)...")
    recent_data = query_tool.get_recent_data(24)
    if not recent_data.empty:
        print(f"   Found {len(recent_data)} recently ingested records")
    
    # Example 5: Custom analysis functions
    print("\n5️⃣ Running custom analyses...")
    
    # Load full dataset for custom analyses
    df = query_tool.load_data(max_files=10)  # Limit for demo
    
    if not df.empty:
        # Run custom analyses
        query_tool.run_custom_query(analyze_entity_types)
        query_tool.run_custom_query(analyze_company_distribution_by_postal_code)
        query_tool.run_custom_query(find_recently_registered_companies)
        
        # Example with parameters
        def street_analysis(df):
            return find_companies_by_street(df, 'ORCHARD')
        
        query_tool.run_custom_query(street_analysis)
    
    print("\n✨ Examples completed!")
    print("\n💡 Usage Tips:")
    print("   - Use command line: python query_data_lake.py summary")
    print("   - Search: python query_data_lake.py search --search-term 'TECH'")
    print("   - Filter: python query_data_lake.py filter --filter '{\"entity_type\": \"PRIVATE LIMITED COMPANY\"}'")
    print("   - Export: python query_data_lake.py export --output companies.csv")

if __name__ == "__main__":
    main()