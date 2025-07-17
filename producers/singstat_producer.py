import os
from datetime import datetime, timedelta
from typing import Dict, Any, List

from loguru import logger

from base_producer import BaseProducer
from models.data_record import DataRecord


class SingStatProducer(BaseProducer):
    """Producer for SingStat (Singapore Department of Statistics) data"""
    
    def __init__(self, kafka_config: Dict[str, Any]):
        super().__init__(
            kafka_config=kafka_config,
            topic='singstat-economics',
            source_name='SingStat'
        )
        self.base_url = os.getenv('SINGSTAT_API_URL', 'https://data.gov.sg/api/action/datastore_search')
        self.resource_discovery_url = os.getenv('SINGSTAT_RESOURCE_DISCOVERY_URL', 'https://api-open.data.gov.sg/v1/public/api/resourceid')
        
        # We'll discover resource IDs dynamically from the API
        self.target_datasets = [
            'GDP',
            'Consumer Price Index', 
            'CPI',
            'Unemployment',
            'Population',
            'Business Expectations',
            'Economic Indicators'
        ]
        
    def extract_data(self) -> None:
        """Extract economic data from SingStat API"""
        try:
            # Get predefined resource IDs for economic data
            resource_ids = self._get_predefined_resource_ids()
            
            if not resource_ids:
                logger.warning("No SingStat resource IDs available")
                return
            
            # Extract data for each dataset
            for resource_id in resource_ids:
                dataset_name = f"economic_data_{resource_id[-8:]}"
                logger.info(f"Extracting {dataset_name} data from SingStat (Resource ID: {resource_id})")
                self._extract_dataset(dataset_name, resource_id)
                
        except Exception as e:
            logger.error(f"Error extracting SingStat data: {e}")
            raise
    
    def _get_predefined_resource_ids(self) -> List[str]:
        """Get predefined SingStat resource IDs for economic data"""
        logger.info("Using predefined SingStat resource IDs for economic data")
        
        # Predefined economic dataset IDs from data.gov.sg
        economic_datasets = [
            'd_cfac5532cbdedecd28c3fcbaafda9f97',  # Share Of Expenditure On GDP At Current Prices
            'd_4e48eb19d0e4dcb8f717a83b78ff5463',  # Generation Of Income Account At Current Prices
            'd_9c0e02a3653b6b73cd3b7b6ec4263f71',  # Government Overall Fiscal Position (% Of GDP)
            'd_3844a22c30957a2f57f2473080e8b922',  # Foreign Direct Investment In Singapore
            'd_853d1c7bef03bb6d4fc04aafd8f0ce5e'   # Singapore's Inward Direct Investment Flows
        ]
        
        logger.info(f"Using {len(economic_datasets)} predefined SingStat datasets")
        return economic_datasets
    

    
    def _get_metadata(self, resource_id: str) -> Dict[str, Any]:
        """Get metadata for a specific resource ID"""
        try:
            # For data.gov.sg API, metadata is included in the main response
            logger.info(f"Metadata will be extracted from main API response for resource {resource_id}")
            return {}
            
        except Exception as e:
            logger.error(f"Error fetching metadata for {resource_id}: {e}")
            return {}
    
    def _extract_dataset(self, dataset_name: str, resource_id: str) -> None:
        """Extract data for a specific dataset"""
        try:
            # Get metadata first
            metadata = self._get_metadata(resource_id)
            
            # Fetch table data using data.gov.sg API
            url = self.base_url
            params = {'resource_id': resource_id, 'limit': 100}
            
            logger.info(f"Fetching table data for {dataset_name} (Resource ID: {resource_id})")
            raw_data = self.fetch_data(url, params=params)
            
            if 'result' not in raw_data:
                logger.warning(f"No data found for {dataset_name}")
                return
            
            result = raw_data['result']
            if not result or 'records' not in result:
                logger.warning(f"Empty data for {dataset_name}")
                return
            
            records = result['records']
            total_records = len(records)
            
            logger.info(f"Processing {total_records} records for {dataset_name}")
            
            # Process each record
            for i, record in enumerate(records):
                try:
                    transformed_record = self.transform_data(record, dataset_name, metadata)
                    # Use dataset_name + record index as key for partitioning
                    key = f"{dataset_name}_{i}"
                    self.send_to_kafka(transformed_record, key=key)
                except Exception as e:
                    logger.error(f"Failed to process {dataset_name} record {i}: {e}")
                    continue
            
            logger.info(f"Total {dataset_name} records processed: {total_records}")
            
        except Exception as e:
            logger.error(f"Error extracting {dataset_name} data: {e}")
            raise
    
    def transform_data(self, raw_data: Dict[str, Any], dataset_name: str, metadata: Dict[str, Any] = None) -> DataRecord:
        """Transform SingStat raw data into standardized format"""
        try:
            # Extract data series name
            data_series = raw_data.get('DataSeries', 'Unknown Series')
            
            # Extract year-value pairs from the record
            year_data = {}
            for key, value in raw_data.items():
                if key.isdigit() and len(key) == 4:  # Year columns (e.g., '2023', '2024')
                    year_data[key] = value
            
            # Common fields across datasets
            processed_data = {
                'dataset_type': dataset_name,
                'data_series': data_series,
                'year_data': year_data,
                'latest_year': max(year_data.keys()) if year_data else '',
                'latest_value': year_data.get(max(year_data.keys())) if year_data else '',
                'total_years': len(year_data),
                'resource_id': raw_data.get('resource_id', '')
            }
            
            data_record = DataRecord(
                source=self.source_name,
                timestamp=datetime.now(),
                data_type="economic_indicator",
                raw_data=raw_data,
                processed_data=processed_data,
                record_id=raw_data.get('_id', ''),
                processing_notes=f'Extracted {dataset_name} data with {len(year_data)} years of data'
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
                raw_data=raw_data,
                processed_data={},
                record_id=raw_data.get('_id', ''),
                validation_errors=[str(e)]
            )


if __name__ == "__main__":
    # Configuration
    kafka_config = {
        'bootstrap_servers': [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:30092')]
    }
    
    # Initialize and run producer
    producer = SingStatProducer(kafka_config)
    try:
        producer.run_extraction()
    finally:
        producer.close()