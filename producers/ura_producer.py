import os
from datetime import datetime, timedelta
from typing import Dict, Any, List

from loguru import logger

from base_producer import BaseProducer
from models.data_record import DataRecord


class URAProducer(BaseProducer):
    """Producer for URA (Urban Redevelopment Authority) data"""
    
    def __init__(self, kafka_config: Dict[str, Any]):
        super().__init__(
            kafka_config=kafka_config,
            topic='ura-geospatial',
            source_name='URA'
        )
        self.token_url = "https://eservice.ura.gov.sg/uraDataService/insertNewToken/v1"
        self.data_url = "https://eservice.ura.gov.sg/uraDataService/invokeUraDS/v1"
        self.access_key = os.getenv('URA_ACCESS_KEY', '99e4b0fe-aae4-4605-94b2-d480e69b8c65')  # Default from user example
        self.token = None
        
        # Available URA services based on API testing
        # Only using PMI_Resi_Rental_Median for consistent data structure
        self.services = [
            'PMI_Resi_Rental_Median',  # Private residential rental median (confirmed working)
        ]
        
    def extract_data(self) -> None:
        """Extract geospatial data from URA API"""
        try:
            # Get authentication token first
            self._get_token()
            
            if not self.token:
                logger.error("Failed to obtain URA API token")
                return
            
            # Extract data for each service
            for service in self.services:
                logger.info(f"Extracting {service} data from URA")
                self._extract_service_data(service)
                
        except Exception as e:
            logger.error(f"Error extracting URA data: {e}")
            raise
    
    def _get_token(self) -> None:
        """Get authentication token from URA API"""
        try:
            headers = {
                'AccessKey': self.access_key,
                'User-Agent': 'Mozilla/5.0 (compatible; DataProducer/1.0)'
            }
            
            logger.info(f"Requesting URA API token with access key: {self.access_key[:8]}...")
            
            # Make direct request to handle potential non-JSON responses
            import requests
            response = requests.get(self.token_url, headers=headers, timeout=30)
            
            logger.info(f"URA token response status: {response.status_code}")
            logger.info(f"URA token response headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    logger.info(f"URA token response data: {response_data}")
                    
                    if response_data.get('Status') == 'Success':
                        self.token = response_data.get('Result')
                        logger.info("Successfully obtained URA API token")
                    else:
                        logger.error(f"Failed to get URA token: {response_data.get('Message', 'Unknown error')}")
                except ValueError as json_error:
                    logger.error(f"URA API returned non-JSON response: {response.text[:200]}")
                    logger.error(f"JSON decode error: {json_error}")
            else:
                logger.error(f"URA API returned status {response.status_code}: {response.text[:200]}")
                
        except Exception as e:
            logger.error(f"Error getting URA token: {e}")
            self.token = None
    
    def _extract_service_data(self, service: str) -> None:
        """Extract data for a specific URA service"""
        try:
            headers = {
                'AccessKey': self.access_key,
                'Token': self.token,
                'User-Agent': 'Mozilla/5.0 (compatible; DataProducer/1.0)'
            }
            
            params = {
                'service': service
            }
            
            # Add required parameters based on service type
            if 'Rental' in service or 'Transaction' in service:
                # Use current quarter format (YYMM) for rental and transaction data
                from datetime import datetime
                current_date = datetime.now()
                ref_period = f"{current_date.strftime('%y')}{current_date.month:02d}"
                params['refPeriod'] = ref_period
                logger.info(f"Using refPeriod: {ref_period} for {service}")
            elif 'Car_Park' in service:
                # Car park services don't require refPeriod but may need other parameters
                logger.info(f"Car park service {service} - no refPeriod required")
            
            logger.info(f"Fetching {service} data from URA with params: {params}")
            
            # Make direct request to handle potential non-JSON responses
            import requests
            response = requests.get(self.data_url, params=params, headers=headers, timeout=30)
            
            logger.info(f"URA {service} response status: {response.status_code}")
            logger.info(f"URA {service} response headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                try:
                    raw_data = response.json()
                    logger.info(f"URA {service} response data keys: {list(raw_data.keys()) if isinstance(raw_data, dict) else 'Not a dict'}")
                    
                    if raw_data.get('Status') != 'Success':
                        logger.error(f"URA API error for {service}: {raw_data.get('Message', 'Unknown error')}")
                        return
                    
                    result = raw_data.get('Result', [])
                    if not result:
                        logger.info(f"No data found for {service}")
                        return
                    
                    total_records = len(result)
                    logger.info(f"Processing {total_records} records for {service}")
                    
                    # Process each record
                    for i, record in enumerate(result):
                        try:
                            transformed_record = self.transform_data(record, service)
                            # Use service + record index as key for partitioning
                            key = f"{service}_{i}"
                            self.send_to_kafka(transformed_record, key=key)
                        except Exception as e:
                            logger.error(f"Failed to process {service} record {i}: {e}")
                            continue
                    
                    logger.info(f"Total {service} records processed: {total_records}")
                    
                except ValueError as json_error:
                    logger.error(f"URA {service} API returned non-JSON response: {response.text[:200]}")
                    logger.error(f"JSON decode error: {json_error}")
            else:
                logger.error(f"URA {service} API returned status {response.status_code}: {response.text[:200]}")
            
        except Exception as e:
            logger.error(f"Error extracting {service} data: {e}")
            raise
    
    def transform_data(self, raw_data: Dict[str, Any], service_name: str) -> DataRecord:
        """Transform URA PMI_Resi_Rental_Median data into standardized format"""
        try:
            # PMI_Resi_Rental_Median specific data structure - only fields from original source
            geospatial_data = {
                'service_type': service_name,
                'street_name': raw_data.get('street', ''),
                'project_name': raw_data.get('project', ''),
                'latitude': raw_data.get('y', ''),
                'longitude': raw_data.get('x', ''),
                # Initialize rental fields from rentalMedian array
                'district': '',
                'rental_median': '',
                'rental_psf25': '',
                'rental_psf75': '',
                'ref_period': ''
            }
            
            # Extract rental median data if available
            rental_median_data = raw_data.get('rentalMedian', [])
            if rental_median_data and len(rental_median_data) > 0:
                # Use the most recent rental data (first item)
                latest_rental = rental_median_data[0]
                geospatial_data.update({
                    'district': latest_rental.get('district', ''),
                    'rental_median': latest_rental.get('median', ''),
                    'rental_psf25': latest_rental.get('psf25', ''),
                    'rental_psf75': latest_rental.get('psf75', ''),
                    'ref_period': latest_rental.get('refPeriod', '')
                })
            
            # Generate record ID using street and project for PMI_Resi_Rental_Median
            record_id = f"{service_name}_{raw_data.get('street', '')}_{raw_data.get('project', '')}"
            
            data_record = DataRecord(
                source=self.source_name,
                timestamp=datetime.now(),
                data_type="geospatial",
                raw_data=raw_data,
                processed_data=geospatial_data,
                record_id=record_id,
                processing_notes=f'Extracted from URA {service_name} service'
            )
            
            # Calculate quality score
            data_record.validate_quality()
            
            return data_record
            
        except Exception as e:
            logger.error(f"Error transforming URA data: {e}")
            return DataRecord(
                source=self.source_name,
                timestamp=datetime.now(),
                data_type="geospatial",
                raw_data=raw_data,
                processed_data={},
                record_id=f"{service_name}_error",
                validation_errors=[str(e)]
            )


if __name__ == "__main__":
    # Configuration
    kafka_config = {
        'bootstrap_servers': [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:30092')]
    }
    
    # Initialize and run producer
    producer = URAProducer(kafka_config)
    try:
        producer.run_extraction()
    finally:
        producer.close()