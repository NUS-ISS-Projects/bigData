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
        
        # Available URA services based on API documentation
        # Prioritizing services most relevant to business analysis
        self.services = [
            'PMI_Resi_Rental_Median',  # Private residential rental median (proposal requirement)
            'PMI_Resi_Transaction',    # Private residential transactions
            'Car_Park_Availability',   # Real-time car park availability (business activity proxy)
            'Car_Park_Details',        # Car park details and locations
            'PMI_Resi_Rental',         # Private residential rental
            'PMI_Resi_Launch_Units',   # Private residential launch units
            'PMI_Resi_Launch_Projects' # Private residential launch projects
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
        """Transform URA raw data into standardized format"""
        try:
            # Common fields across services
            geospatial_data = {
                'service_type': service_name,
                'postal_code': raw_data.get('postalCode', raw_data.get('postal_code', '')),
                'planning_area': raw_data.get('planningArea', raw_data.get('planning_area', '')),
                'planning_region': raw_data.get('planningRegion', raw_data.get('planning_region', '')),
                'subzone': raw_data.get('subzone', ''),
                'street_name': raw_data.get('street', raw_data.get('streetName', raw_data.get('street_name', ''))),
                'project_name': raw_data.get('project', raw_data.get('projectName', raw_data.get('project_name', ''))),
                'transaction_date': raw_data.get('contractDate', raw_data.get('transactionDate', raw_data.get('date', ''))),
                'latitude': raw_data.get('y', raw_data.get('latitude', '')),
                'longitude': raw_data.get('x', raw_data.get('longitude', ''))
            }
            
            # Service-specific transformations based on URA API structure
            if 'Rental' in service_name:
                geospatial_data.update({
                    'property_type': raw_data.get('propertyType', ''),
                    'district': raw_data.get('district', ''),
                    'rental_median': raw_data.get('median', ''),
                    'rental_max': raw_data.get('max', ''),
                    'rental_min': raw_data.get('min', ''),
                    'lease_term': raw_data.get('leaseTerm', ''),
                    'area_sqm': raw_data.get('areaSqm', ''),
                    'rental_psm': raw_data.get('rentalPsm', '')
                })
            elif 'Transaction' in service_name:
                geospatial_data.update({
                    'property_type': raw_data.get('propertyType', ''),
                    'district': raw_data.get('district', ''),
                    'type_of_sale': raw_data.get('typeOfSale', ''),
                    'price': raw_data.get('price', ''),
                    'area_sqm': raw_data.get('areaSqm', ''),
                    'unit_price_psm': raw_data.get('unitPrice', ''),
                    'nett_price': raw_data.get('nettPrice', ''),
                    'floor_area_sqm': raw_data.get('floorArea', ''),
                    'type_of_area': raw_data.get('typeOfArea', ''),
                    'tenure': raw_data.get('tenure', '')
                })
            elif 'Launch' in service_name:
                geospatial_data.update({
                    'property_type': raw_data.get('propertyType', ''),
                    'district': raw_data.get('district', ''),
                    'launch_date': raw_data.get('launchDate', ''),
                    'units_launched': raw_data.get('unitsLaunched', ''),
                    'units_sold': raw_data.get('unitsSold', ''),
                    'median_price': raw_data.get('medianPrice', ''),
                    'max_price': raw_data.get('maxPrice', ''),
                    'min_price': raw_data.get('minPrice', '')
                })
            elif 'Car_Park' in service_name:
                geospatial_data.update({
                    'car_park_no': raw_data.get('carParkNo', raw_data.get('carparkNo', '')),
                    'area': raw_data.get('area', ''),
                    'development': raw_data.get('development', ''),
                    'location': raw_data.get('location', ''),
                    'available_lots': raw_data.get('availableLots', ''),
                    'lot_type': raw_data.get('lotType', ''),
                    'agency': raw_data.get('agency', ''),
                    'car_park_type': raw_data.get('carParkType', ''),
                    'type_of_parking_system': raw_data.get('typeOfParkingSystem', ''),
                    'short_term_parking': raw_data.get('shortTermParking', ''),
                    'free_parking': raw_data.get('freeParking', ''),
                    'night_parking': raw_data.get('nightParking', ''),
                    'car_park_decks': raw_data.get('carParkDecks', ''),
                    'gantry_height': raw_data.get('gantryHeight', ''),
                    'car_park_basement': raw_data.get('carParkBasement', '')
                })
            
            # Generate record ID
            record_id = f"{service_name}_{raw_data.get('postalCode', raw_data.get('carParkNo', 'unknown'))}"
            
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