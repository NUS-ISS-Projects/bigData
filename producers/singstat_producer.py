import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from urllib.request import Request, urlopen

from loguru import logger

from base_producer import BaseProducer
from models.data_record import DataRecord


class SingStatProducer(BaseProducer):
    """Producer for SingStat (Singapore Department of Statistics) data using official Table Builder API"""
    
    def __init__(self, kafka_config: Dict[str, Any]):
        super().__init__(
            kafka_config=kafka_config,
            topic='singstat-economics',
            source_name='SingStat'
        )
        self.base_url = 'https://tablebuilder.singstat.gov.sg/api/table'
        self.headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        
        # Diverse economic indicators resource IDs with actual time series data
        # Expanded beyond GDP to include comprehensive economic categories
        
        # GDP & National Accounts (validated existing resources)
        self.gdp_resources = [
            'M014811',  # Expenditure On Gross Domestic Product In Chained (2015) Dollars, Quarterly
            'M014812',  # Expenditure On Gross Domestic Product At Current Market Prices, Quarterly
            'M014871',  # Gross Domestic Product By Industry At Current Basic Prices, Quarterly
            'M014911',  # Gross Domestic Product By Industry In Chained (2015) Dollars, Quarterly
            'M014921',  # Gross Domestic Product By Industry At Current Basic Prices, Annual
            'M015721',  # Gross Domestic Product At Current Market Prices, Annual
            'M015731',  # Gross Domestic Product In Chained (2015) Dollars, Annual
        ]
        
        # High-priority diverse indicators (630+ data points)
        self.high_priority_diverse = [
            'M602091',  # Ratio Of Top 3 Operating Expenditure Items For Selected Services Industries (630 points)
            'M601481',  # Key Indicators By Detailed Industry In All Services Industries (362 points)
            'M602081',  # Key Indicators Of The Services Industries By Size Of Operating Revenue (359 points)
        ]
        
        # Trade & External Relations
        self.trade_external = [
            '17947',    # Indonesia Merchandise Trade by Region/Market (260 points)
            '17949',    # Malaysia Merchandise Trade by Region/Market (260 points)
            '17951',    # Singapore Merchandise Trade by Region/Market (260 points)
            'M083811',  # Foreign Direct Investment In Singapore By Source Economy (238 points)
            'M060261',  # Exports Of Services By Major Trading Partner (237 points)
            'M060271',  # Imports Of Services By Major Trading Partner (229 points)
        ]
        
        # Employment & Labor
        self.employment_labor = [
            'M601471',  # Key Indicators By Industry Group In All Services Industries (209 points)
        ]
        
        # Inflation & Prices
        self.inflation_prices = [
            'M213801',  # Consumer Price Index (CPI), 2024 As Base Year (207 points)
            'M213811',  # Percent Change In Consumer Price Index Over Previous Year (164 points)
            'M213071',  # Consumer Price Index (CPI) - Additional comprehensive dataset
        ]
        
        # Demographics & Population
        self.demographics_population = [
            'M810381',  # Singapore Residents By Age Group And Type Of Dwelling (180 points)
            'M810011',  # Population and demographic indicators - Comprehensive dataset
        ]
        
        # Government & Public Finance
        self.government_public = [
            'M060291',  # Singapore's Portfolio Investment Assets By Destination Economy (180 points)
        ]
        
        # Comprehensive resource list (all categories)
        self.economic_resource_ids = (
            self.gdp_resources +
            self.high_priority_diverse +
            self.trade_external +
            self.employment_labor +
            self.inflation_prices +
            self.demographics_population +
            self.government_public
        )
        
        # Resource categories for organized processing
        self.resource_categories = {
            'GDP & National Accounts': self.gdp_resources,
            'Services & Tourism': self.high_priority_diverse,
            'Trade & External Relations': self.trade_external,
            'Employment & Labor': self.employment_labor,
            'Inflation & Prices': self.inflation_prices,
            'Demographics & Population': self.demographics_population,
            'Government & Public Finance': self.government_public
        }
        
    def extract(self) -> None:
        """Main extraction method with comprehensive resource discovery"""
        try:
            # Use comprehensive extraction with discovery for maximum data coverage
            self.extract_with_discovery(include_discovered=True, max_discovered=100)
            
        except Exception as e:
            logger.error(f"Error in SingStat extraction: {e}")
            raise
    
    def extract_data(self, categories: List[str] = None) -> None:
        """Extract economic data from SingStat Table Builder API
        
        Args:
            categories: List of category names to extract. If None, extracts all categories.
                       Available: ['GDP & National Accounts', 'Services & Tourism', 
                                  'Trade & External Relations', 'Employment & Labor',
                                  'Inflation & Prices', 'Demographics & Population',
                                  'Government & Public Finance']
        """
        try:
            # Determine which resources to extract
            if categories:
                resource_ids = []
                for category in categories:
                    if category in self.resource_categories:
                        resource_ids.extend(self.resource_categories[category])
                        logger.info(f"Including {len(self.resource_categories[category])} resources from '{category}'")
                    else:
                        logger.warning(f"Unknown category: {category}")
            else:
                resource_ids = self.economic_resource_ids
                logger.info("Extracting all available economic indicators")
            
            logger.info(f"Starting SingStat data extraction for {len(resource_ids)} datasets")
            logger.info(f"Resource IDs to process: {resource_ids}")
            
            # Extract data for each resource ID with category context
            for i, resource_id in enumerate(resource_ids, 1):
                try:
                    # Find which category this resource belongs to
                    category = self._get_resource_category(resource_id)
                    logger.info(f"Processing resource {i}/{len(resource_ids)}: {resource_id} (Category: {category})")
                    self._extract_dataset(resource_id)
                except Exception as e:
                    logger.error(f"Error extracting resource {resource_id}: {e}")
                    # Continue with next resource instead of failing completely
                    continue
                
            logger.info("Completed SingStat data extraction")
                
        except Exception as e:
            logger.error(f"Error extracting SingStat data: {e}")
            raise
    
    def extract_basic(self) -> None:
        """Basic extraction method that processes only predefined economic resource IDs"""
        try:
            logger.info(f"Starting basic SingStat data extraction for {len(self.economic_resource_ids)} datasets")
            
            for i, resource_id in enumerate(self.economic_resource_ids, 1):
                try:
                    category = self._get_resource_category(resource_id)
                    logger.info(f"Processing resource {i}/{len(self.economic_resource_ids)}: {resource_id} (Category: {category})")
                    self._extract_dataset(resource_id)
                    
                except Exception as e:
                    logger.error(f"Error extracting resource {resource_id}: {e}")
                    continue
            
            logger.info(f"Completed basic SingStat data extraction for {len(self.economic_resource_ids)} datasets")
            
        except Exception as e:
            logger.error(f"Error in basic SingStat extraction: {e}")
            raise
    
    def _get_resource_category(self, resource_id: str) -> str:
        """Get the category name for a given resource ID"""
        for category, resources in self.resource_categories.items():
            if resource_id in resources:
                return category
        return "Unknown"
    
    def get_category_summary(self) -> Dict[str, Dict[str, Any]]:
        """Get summary of available categories and their resource counts"""
        summary = {}
        for category, resources in self.resource_categories.items():
            summary[category] = {
                'resource_count': len(resources),
                'resource_ids': resources,
                'description': self._get_category_description(category)
            }
        return summary
    
    def _get_category_description(self, category: str) -> str:
        """Get description for each category"""
        descriptions = {
            'GDP & National Accounts': 'Gross Domestic Product and national economic accounts',
            'Services & Tourism': 'Service industry indicators and tourism-related metrics',
            'Trade & External Relations': 'International trade, foreign investment, and external economic relations',
            'Employment & Labor': 'Labor market indicators and employment statistics',
            'Inflation & Prices': 'Consumer price indices and inflation measures',
            'Demographics & Population': 'Population statistics and demographic indicators',
            'Government & Public Finance': 'Government financial data and public sector indicators'
        }
        return descriptions.get(category, 'Economic indicators')
    
    def _make_api_request(self, url: str) -> Dict[str, Any]:
        """Make API request to SingStat Table Builder"""
        try:
            request = Request(url, headers=self.headers)
            response = urlopen(request)
            data = response.read().decode('utf-8')
            return json.loads(data)
        except Exception as e:
            logger.error(f"API request failed for {url}: {e}")
            raise
    

    
    def _get_metadata(self, resource_id: str) -> Dict[str, Any]:
        """Get metadata for a specific resource ID from SingStat Table Builder"""
        try:
            url = f"{self.base_url}/metadata/{resource_id}"
            logger.info(f"Fetching metadata for resource {resource_id}")
            
            metadata_response = self._make_api_request(url)
            
            if metadata_response.get('StatusCode') == 200 and 'Data' in metadata_response:
                return metadata_response['Data']['records']
            else:
                logger.warning(f"No metadata found for resource {resource_id}")
                return {}
            
        except Exception as e:
            logger.error(f"Error fetching metadata for {resource_id}: {e}")
            return {}
    
    def _extract_dataset(self, resource_id: str) -> None:
        """Extract data for a specific resource ID from SingStat Table Builder with pagination"""
        try:
            # Get metadata first
            metadata = self._get_metadata(resource_id)
            dataset_title = metadata.get('title', f'Dataset_{resource_id}')
            
            logger.info(f"Extracting data for: {dataset_title} (ID: {resource_id})")
            
            # Initialize pagination variables
            offset = 0
            limit = 5000  # API maximum
            total_records_processed = 0
            all_records = []
            
            while True:
                # Fetch table data with pagination
                url = f"{self.base_url}/tabledata/{resource_id}?limit={limit}&offset={offset}&sortBy=rowtext%20asc"
                logger.info(f"Fetching data for {resource_id} - offset: {offset}, limit: {limit}")
                
                table_data_response = self._make_api_request(url)
                
                if table_data_response.get('StatusCode') != 200 or 'Data' not in table_data_response:
                    if offset == 0:
                        logger.warning(f"No table data found for resource {resource_id}")
                        return
                    else:
                        # No more data available
                        break
                
                # Process table data using updated method
                records = self.process_table_data(resource_id, table_data_response, metadata)
                
                if not records:
                    # No more records in this batch
                    break
                
                all_records.extend(records)
                batch_size = len(records)
                total_records_processed += batch_size
                
                logger.info(f"Processed batch of {batch_size} records for {dataset_title} (total: {total_records_processed})")
                
                # Check if we got fewer records than the limit (indicates last page)
                if batch_size < limit:
                    break
                
                # Move to next page
                offset += limit
            
            if not all_records:
                logger.warning(f"No records processed for resource {resource_id}")
                return
            
            logger.info(f"Processing {len(all_records)} total records for {dataset_title}")
            
            # Send each record to Kafka
            for i, record in enumerate(all_records):
                try:
                    # Use resource_id + record index as key for partitioning
                    key = f"{resource_id}_{i}"
                    self.send_to_kafka(record, key=key)
                except Exception as e:
                    logger.error(f"Failed to send {resource_id} record {i} to Kafka: {e}")
                    continue
            
            logger.info(f"Total records processed for {dataset_title}: {len(all_records)}")
            
        except Exception as e:
            logger.error(f"Error extracting data for resource {resource_id}: {e}")
            raise
    
    def process_table_data(self, resource_id: str, table_data: Dict[str, Any], metadata: Dict[str, Any] = None) -> List[DataRecord]:
        """Process table data into standardized DataRecord format"""
        records = []
        
        try:
            if 'Data' not in table_data:
                logger.warning(f"No Data section found for resource {resource_id}")
                return records
                
            data_section = table_data['Data']
            
            # Handle SingStat time series structure with 'row' arrays
            if isinstance(data_section, dict) and 'row' in data_section:
                rows = data_section['row']
                if isinstance(rows, list):
                    for row in rows:
                        if isinstance(row, dict):
                            # Transform each row using existing transform_data method
                            record = self.transform_data(row, resource_id, metadata, data_section)
                            if record:
                                records.append(record)
                                        
            # Handle other data structures (fallback)
            elif isinstance(data_section, list):
                # Direct list of records
                for item in data_section:
                    if isinstance(item, dict):
                        record = self.transform_data(item, resource_id, None, data_section)
                        if record:
                            records.append(record)
                            
            elif isinstance(data_section, dict):
                # Check for nested records
                if 'records' in data_section:
                    nested_records = data_section['records']
                    if isinstance(nested_records, list):
                        for item in nested_records:
                            record = self.transform_data(item, resource_id, None, data_section)
                            if record:
                                records.append(record)
                    elif isinstance(nested_records, dict):
                        record = self.transform_data(nested_records, resource_id, None, data_section)
                        if record:
                            records.append(record)
                else:
                    # Try to process the data section directly
                    record = self.transform_data(data_section, resource_id, None, data_section)
                    if record:
                        records.append(record)
                        
        except Exception as e:
            logger.error(f"Error processing table data for {resource_id}: {e}")
            
        return records
    
    def transform_data(self, row_data: Dict[str, Any], resource_id: str, metadata: Dict[str, Any] = None, table_data: Dict[str, Any] = None) -> DataRecord:
        """Transform SingStat Table Builder data into standardized format"""
        try:
            # Extract metadata information
            dataset_title = metadata.get('title', f'Dataset_{resource_id}') if metadata else f'Dataset_{resource_id}'
            frequency = metadata.get('frequency', 'Unknown') if metadata else 'Unknown'
            data_source = metadata.get('dataSource', 'SingStat') if metadata else 'SingStat'
            
            # Extract row information
            series_no = row_data.get('seriesNo', '')
            row_text = row_data.get('rowText', '')
            unit_of_measure = row_data.get('uoM', '')
            
            # Extract time series data from columns with proper key-value handling
            time_series_data = {}
            columns = row_data.get('columns', [])
            
            for column in columns:
                if isinstance(column, dict):
                    time_key = column.get('key', '')
                    value = column.get('value', '')
                    if time_key and value:
                        # Convert value to appropriate type if possible
                        try:
                            # Try to convert to float for numeric data
                            numeric_value = float(value)
                            time_series_data[time_key] = numeric_value
                        except (ValueError, TypeError):
                            # Keep as string if not numeric
                            time_series_data[time_key] = value
            
            # Get latest data point
            latest_period = ''
            latest_value = ''
            if time_series_data:
                # Sort by time key to get the latest
                sorted_periods = sorted(time_series_data.keys())
                latest_period = sorted_periods[-1] if sorted_periods else ''
                latest_value = time_series_data.get(latest_period, '')
            
            # Extract additional table metadata if available
            table_title = ''
            table_frequency = ''
            table_datasource = ''
            if table_data and isinstance(table_data, dict):
                table_title = table_data.get('title', '')
                table_frequency = table_data.get('frequency', '')
                table_datasource = table_data.get('datasource', '')
            
            # Common fields across datasets
            processed_data = {
                'resource_id': resource_id,
                'dataset_title': dataset_title or table_title,
                'frequency': frequency or table_frequency,
                'data_source': data_source or table_datasource,
                'series_no': series_no,
                'indicator_name': row_text,
                'unit_of_measure': unit_of_measure,
                'time_series_data': time_series_data,
                'latest_period': latest_period,
                'latest_value': latest_value,
                'total_periods': len(time_series_data),
                'data_last_updated': metadata.get('dataLastUpdated', '') if metadata else '',
                'table_title': table_title
            }
            
            # Create unique record ID
            record_id = f"{resource_id}_{series_no}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            data_record = DataRecord(
                source=self.source_name,
                timestamp=datetime.now(),
                data_type="economic_indicator",
                raw_data={
                    'resource_id': resource_id,
                    'metadata': metadata,
                    'row_data': row_data,
                    'table_info': {
                        'title': table_title,
                        'frequency': table_frequency,
                        'datasource': table_datasource
                    }
                },
                processed_data=processed_data,
                record_id=record_id,
                processing_notes=f'Extracted {dataset_title or table_title} - {row_text} with {len(time_series_data)} data points'
            )
            
            # Calculate quality score
            data_record.validate_quality()
            
            return data_record
            
        except Exception as e:
            logger.error(f"Error transforming SingStat data: {e}")
            return DataRecord(
                source=self.source_name,
                timestamp=datetime.now(),
                data_type="economic_indicator",
                raw_data={'resource_id': resource_id, 'row_data': row_data},
                processed_data={},
                record_id=f"{resource_id}_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                validation_errors=[str(e)]
            )


    def search_resource_ids(self, keyword: str, search_option: str = "all") -> List[Dict[str, Any]]:
        """Search for resource IDs by keyword using SingStat Table Builder API"""
        try:
            url = f"{self.base_url}/resourceid?keyword={keyword}&searchOption={search_option}"
            logger.info(f"Searching for resources with keyword: {keyword}")
            
            response = self._make_api_request(url)
            
            if response.get('StatusCode') == 200 and 'Data' in response:
                records = response['Data'].get('records', [])
                logger.info(f"Found {len(records)} resources for keyword '{keyword}'")
                return records
            else:
                logger.warning(f"No resources found for keyword '{keyword}'")
                return []
                
        except Exception as e:
            logger.error(f"Error searching for resources with keyword '{keyword}': {e}")
            return []
    
    def discover_additional_resources(self, max_per_keyword: int = 50) -> List[str]:
        """Discover additional economic resource IDs using comprehensive keyword search"""
        try:
            logger.info("Starting comprehensive resource discovery...")
            
            # Comprehensive economic keywords
            keywords = [
                'GDP', 'Economic', 'Trade', 'Employment', 'Inflation', 'Population',
                'Investment', 'Services', 'Manufacturing', 'Construction', 'Tourism',
                'Finance', 'Government', 'Revenue', 'Expenditure', 'Income', 'Wages',
                'Productivity', 'Industry', 'Business', 'Consumer', 'Price', 'Index',
                'Export', 'Import', 'Foreign', 'Domestic', 'National', 'Regional'
            ]
            
            discovered_resources = set()
            current_resources = set(self.economic_resource_ids)
            
            for keyword in keywords:
                try:
                    resources = self.search_resource_ids(keyword)
                    count = 0
                    
                    for resource in resources:
                        if count >= max_per_keyword:
                            break
                            
                        resource_id = resource.get('resourceId')
                        if resource_id and resource_id not in current_resources:
                            discovered_resources.add(resource_id)
                            count += 1
                            
                except Exception as e:
                    logger.warning(f"Error searching for keyword '{keyword}': {e}")
                    continue
            
            discovered_list = list(discovered_resources)
            logger.info(f"Discovered {len(discovered_list)} additional resources beyond current {len(current_resources)}")
            
            # Log sample of discovered resources
            if discovered_list:
                sample_size = min(10, len(discovered_list))
                logger.info(f"Sample discovered resources: {discovered_list[:sample_size]}")
            
            return discovered_list
            
        except Exception as e:
            logger.error(f"Error in resource discovery: {e}")
            return []
    
    def extract_with_discovery(self, include_discovered: bool = True, max_discovered: int = 100) -> None:
        """Extract data with optional resource discovery for comprehensive coverage"""
        try:
            resource_ids = list(self.economic_resource_ids)
            
            if include_discovered:
                logger.info("Performing resource discovery for comprehensive data coverage...")
                discovered = self.discover_additional_resources()
                
                # Limit discovered resources to prevent overwhelming the system
                if len(discovered) > max_discovered:
                    logger.info(f"Limiting discovered resources from {len(discovered)} to {max_discovered}")
                    discovered = discovered[:max_discovered]
                
                resource_ids.extend(discovered)
                logger.info(f"Total resources to extract: {len(resource_ids)} (original: {len(self.economic_resource_ids)}, discovered: {len(discovered)})")
            
            # Extract data for all resources
            logger.info(f"Starting comprehensive SingStat data extraction for {len(resource_ids)} datasets")
            
            for i, resource_id in enumerate(resource_ids, 1):
                try:
                    category = self._get_resource_category(resource_id)
                    if category == "Unknown" and include_discovered:
                        category = "Discovered"
                    
                    logger.info(f"Processing resource {i}/{len(resource_ids)}: {resource_id} (Category: {category})")
                    self._extract_dataset(resource_id)
                    
                except Exception as e:
                    logger.error(f"Error extracting resource {resource_id}: {e}")
                    continue
            
            logger.info(f"Completed comprehensive SingStat data extraction for {len(resource_ids)} datasets")
            
        except Exception as e:
            logger.error(f"Error in comprehensive extraction: {e}")
            raise


if __name__ == "__main__":
    # Configuration
    kafka_config = {
        'bootstrap_servers': [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:30092')]
    }
    
    # Initialize and run producer
    producer = SingStatProducer(kafka_config)
    try:
        # Display diverse economic indicators summary
        logger.info("=== SingStat Diverse Economic Indicators Summary ===")
        category_summary = producer.get_category_summary()
        total_resources = sum(cat['resource_count'] for cat in category_summary.values())
        logger.info(f"Total resources available: {total_resources} across {len(category_summary)} categories")
        
        for category, info in category_summary.items():
            logger.info(f"{category}: {info['resource_count']} resources - {info['description']}")
        
        # Example: Extract only specific categories (uncomment to test)
        # logger.info("\n=== Extracting High-Priority Categories ===")
        # producer.extract_data(categories=['GDP & National Accounts', 'Services & Tourism', 'Inflation & Prices'])
        
        # Extract all diverse economic indicators
        logger.info("\n=== Extracting All Diverse Economic Indicators ===")
        producer.run_extraction()
        
        # Test search functionality for additional resources
        logger.info("\n=== Testing Resource Discovery ===")
        gdp_resources = producer.search_resource_ids("GDP")
        employment_resources = producer.search_resource_ids("Employment")
        trade_resources = producer.search_resource_ids("Trade")
        
        logger.info(f"Additional GDP resources found: {len(gdp_resources)}")
        logger.info(f"Additional Employment resources found: {len(employment_resources)}")
        logger.info(f"Additional Trade resources found: {len(trade_resources)}")
        
    finally:
        producer.close()