#!/usr/bin/env python3
"""
Phase A Data Source Verification Script
Tests all three data sources (ACRA, SingStat, URA) with the backend infrastructure
"""

import os
import sys
import time
import json
from datetime import datetime
from typing import Dict, Any, List

# Add producers directory to path
sys.path.append('./producers')

from loguru import logger
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Import our producers
from acra_producer import ACRAProducer
from singstat_producer import SingStatProducer
from ura_producer import URAProducer


class DataSourceTester:
    """Comprehensive tester for all data sources"""
    
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092']
        }
        
        self.test_results = {
            'acra': {'status': 'pending', 'records': 0, 'errors': []},
            'singstat': {'status': 'pending', 'records': 0, 'errors': []},
            'ura': {'status': 'pending', 'records': 0, 'errors': []}
        }
        
        self.start_time = datetime.now()
        
        # Configure logging
        logger.add(
            "test_results_{time:YYYY-MM-DD_HH-mm-ss}.log",
            rotation="1 day",
            level="INFO"
        )
    
    def test_kafka_connectivity(self) -> bool:
        """Test Kafka connectivity"""
        try:
            logger.info("Testing Kafka connectivity...")
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                consumer_timeout_ms=5000
            )
            
            # Test basic connectivity by getting cluster metadata
            cluster_metadata = consumer._client.cluster
            available_topics = cluster_metadata.topics()
            consumer.close()
            
            logger.info(f"✅ Kafka connectivity successful - {len(available_topics)} topics available")
            return True
            
        except Exception as e:
            logger.error(f"❌ Kafka connectivity failed: {e}")
            return False
    
    def test_acra_producer(self) -> Dict[str, Any]:
        """Test ACRA data source"""
        logger.info("\n" + "="*50)
        logger.info("Testing ACRA Data Source")
        logger.info("="*50)
        
        try:
            # Create producer with limited extraction for testing
            producer = ACRAProducer(self.kafka_config)
            
            # Override extract_data method for testing (limit to 10 records)
            original_extract = producer.extract_data
            
            def limited_extract():
                try:
                    params = {
                        'resource_id': producer.resource_id,
                        'limit': 10,  # Limit for testing
                        'offset': 0
                    }
                    
                    logger.info(f"Fetching ACRA test data: limit=10")
                    raw_data = producer.fetch_data(producer.base_url, params=params)
                    
                    if not raw_data.get('result', {}).get('records'):
                        logger.warning("No ACRA records found")
                        return
                    
                    records = raw_data['result']['records']
                    record_count = len(records)
                    
                    logger.info(f"Processing {record_count} ACRA test records")
                    
                    for i, record in enumerate(records):
                        try:
                            transformed_record = producer.transform_data(record)
                            key = record.get('uen', f'test_{i}')
                            producer.send_to_kafka(transformed_record, key=key)
                            self.test_results['acra']['records'] += 1
                        except Exception as e:
                            error_msg = f"Failed to process ACRA record {i}: {e}"
                            logger.error(error_msg)
                            self.test_results['acra']['errors'].append(error_msg)
                    
                    logger.info(f"ACRA test completed: {self.test_results['acra']['records']} records processed")
                    
                except Exception as e:
                    error_msg = f"ACRA extraction error: {e}"
                    logger.error(error_msg)
                    self.test_results['acra']['errors'].append(error_msg)
                    raise
            
            # Run limited extraction
            producer.extract_data = limited_extract
            producer.run_extraction()
            
            producer.close()
            
            if self.test_results['acra']['records'] > 0:
                self.test_results['acra']['status'] = 'success'
                logger.info(f"✅ ACRA test successful: {self.test_results['acra']['records']} records")
            else:
                self.test_results['acra']['status'] = 'failed'
                logger.error("❌ ACRA test failed: No records processed")
            
        except Exception as e:
            self.test_results['acra']['status'] = 'failed'
            error_msg = f"ACRA test exception: {e}"
            logger.error(f"❌ {error_msg}")
            self.test_results['acra']['errors'].append(error_msg)
        
        return self.test_results['acra']
    
    def test_singstat_producer(self) -> Dict[str, Any]:
        """Test SingStat data source"""
        logger.info("\n" + "="*50)
        logger.info("Testing SingStat Data Source")
        logger.info("="*50)
        
        try:
            producer = SingStatProducer(self.kafka_config)
            
            # Override extract_data for limited testing
            original_extract = producer.extract_data
            
            def limited_extract():
                try:
                    # Test with predefined resource IDs
                    logger.info("Testing SingStat with predefined resource IDs...")
                    resource_ids = producer._get_predefined_resource_ids()
                    
                    if not resource_ids:
                        logger.warning("No SingStat resources available")
                        return
                    
                    logger.info(f"Available {len(resource_ids)} SingStat resources")
                    
                    # Use first 2 resources for testing
                    test_resources = resource_ids[:2]
                    
                    logger.info(f"Testing with {len(test_resources)} resources")
                    
                    # Extract data for test resources
                    for resource_id in test_resources:
                        dataset_name = f"economic_data_{resource_id[-8:]}"
                        
                        try:
                            logger.info(f"Testing {dataset_name} (ID: {resource_id})")
                            
                            # Get metadata
                            metadata = producer._get_metadata(resource_id)
                            
                            # Fetch limited table data using the correct API endpoint
                            url = producer.base_url
                            params = {'resource_id': resource_id, 'limit': 5}
                            
                            raw_data = producer.fetch_data(url, params=params)
                            
                            if 'result' not in raw_data or 'records' not in raw_data['result']:
                                logger.warning(f"No data found for {dataset_name}")
                                continue
                            
                            records = raw_data['result']['records']  # Get records from result
                            
                            for i, record in enumerate(records):
                                try:
                                    transformed_record = producer.transform_data(record, dataset_name, metadata)
                                    key = f"{dataset_name}_{i}"
                                    producer.send_to_kafka(transformed_record, key=key)
                                    self.test_results['singstat']['records'] += 1
                                except Exception as e:
                                    error_msg = f"Failed to process SingStat record {i}: {e}"
                                    logger.error(error_msg)
                                    self.test_results['singstat']['errors'].append(error_msg)
                            
                            logger.info(f"Processed {len(records)} records for {dataset_name}")
                            
                        except Exception as e:
                            error_msg = f"Error processing {dataset_name}: {e}"
                            logger.error(error_msg)
                            self.test_results['singstat']['errors'].append(error_msg)
                    
                    logger.info(f"SingStat test completed: {self.test_results['singstat']['records']} records processed")
                    
                except Exception as e:
                    error_msg = f"SingStat extraction error: {e}"
                    logger.error(error_msg)
                    self.test_results['singstat']['errors'].append(error_msg)
                    raise
            
            # Run limited extraction
            producer.extract_data = limited_extract
            producer.run_extraction()
            
            producer.close()
            
            if self.test_results['singstat']['records'] > 0:
                self.test_results['singstat']['status'] = 'success'
                logger.info(f"✅ SingStat test successful: {self.test_results['singstat']['records']} records")
            else:
                self.test_results['singstat']['status'] = 'failed'
                logger.error("❌ SingStat test failed: No records processed")
            
        except Exception as e:
            self.test_results['singstat']['status'] = 'failed'
            error_msg = f"SingStat test exception: {e}"
            logger.error(f"❌ {error_msg}")
            self.test_results['singstat']['errors'].append(error_msg)
        
        return self.test_results['singstat']
    
    def test_ura_producer(self) -> Dict[str, Any]:
        """Test URA data source"""
        logger.info("\n" + "="*50)
        logger.info("Testing URA Data Source")
        logger.info("="*50)
        
        try:
            producer = URAProducer(self.kafka_config)
            
            # Override extract_data for limited testing
            original_extract = producer.extract_data
            
            def limited_extract():
                try:
                    # Get authentication token
                    producer._get_token()
                    
                    if not producer.token:
                        logger.error("Failed to obtain URA API token")
                        return
                    
                    logger.info("✅ URA token obtained successfully")
                    
                    # Test all services for comprehensive coverage
                    test_services = producer.services
                    
                    for service in test_services:
                        try:
                            logger.info(f"Testing URA service: {service}")
                            
                            headers = {
                                'AccessKey': producer.access_key,
                                'Token': producer.token,
                                'User-Agent': 'Mozilla/5.0 (compatible; DataProducer/1.0)'
                            }
                            
                            params = {'service': service}
                            
                            # Add required parameters based on service type
                            if 'Rental' in service or 'Transaction' in service:
                                from datetime import datetime
                                current_date = datetime.now()
                                ref_period = f"{current_date.strftime('%y')}{current_date.month:02d}"
                                params['refPeriod'] = ref_period
                                logger.info(f"Using refPeriod: {ref_period} for {service}")
                            
                            logger.info(f"Fetching {service} data from URA with params: {params}")
                            
                            # Make direct request to handle potential non-JSON responses
                            import requests
                            response = requests.get(producer.data_url, params=params, headers=headers, timeout=30)
                            
                            logger.info(f"URA {service} response status: {response.status_code}")
                            logger.info(f"URA {service} response headers: {dict(response.headers)}")
                            
                            if response.status_code != 200:
                                error_msg = f"URA {service} API returned status {response.status_code}: {response.text[:200]}"
                                logger.error(error_msg)
                                self.test_results['ura']['errors'].append(error_msg)
                                continue
                            
                            try:
                                raw_data = response.json()
                                logger.info(f"URA {service} response data keys: {list(raw_data.keys()) if isinstance(raw_data, dict) else 'Not a dict'}")
                            except ValueError as json_error:
                                error_msg = f"URA {service} API returned non-JSON response: {response.text[:200]}"
                                logger.error(error_msg)
                                logger.error(f"JSON decode error: {json_error}")
                                self.test_results['ura']['errors'].append(error_msg)
                                continue
                            
                            if raw_data.get('Status') != 'Success':
                                error_msg = f"URA API error for {service}: {raw_data.get('Message', 'Unknown error')}"
                                logger.error(error_msg)
                                self.test_results['ura']['errors'].append(error_msg)
                                continue
                            
                            result = raw_data.get('Result', [])
                            if not result:
                                logger.info(f"No data found for {service}")
                                continue
                            
                            # Process limited records (max 5)
                            test_records = result[:5]
                            
                            for i, record in enumerate(test_records):
                                try:
                                    transformed_record = producer.transform_data(record, service)
                                    key = f"{service}_{i}"
                                    producer.send_to_kafka(transformed_record, key=key)
                                    self.test_results['ura']['records'] += 1
                                except Exception as e:
                                    error_msg = f"Failed to process URA record {i}: {e}"
                                    logger.error(error_msg)
                                    self.test_results['ura']['errors'].append(error_msg)
                            
                            logger.info(f"Processed {len(test_records)} records for {service}")
                            
                        except Exception as e:
                            error_msg = f"Error processing URA service {service}: {e}"
                            logger.error(error_msg)
                            self.test_results['ura']['errors'].append(error_msg)
                    
                    logger.info(f"URA test completed: {self.test_results['ura']['records']} records processed")
                    
                except Exception as e:
                    error_msg = f"URA extraction error: {e}"
                    logger.error(error_msg)
                    self.test_results['ura']['errors'].append(error_msg)
                    raise
            
            # Run limited extraction
            producer.extract_data = limited_extract
            producer.run_extraction()
            
            producer.close()
            
            if self.test_results['ura']['records'] > 0:
                self.test_results['ura']['status'] = 'success'
                logger.info(f"✅ URA test successful: {self.test_results['ura']['records']} records")
            else:
                self.test_results['ura']['status'] = 'failed'
                logger.error("❌ URA test failed: No records processed")
            
        except Exception as e:
            self.test_results['ura']['status'] = 'failed'
            error_msg = f"URA test exception: {e}"
            logger.error(f"❌ {error_msg}")
            self.test_results['ura']['errors'].append(error_msg)
        
        return self.test_results['ura']
    
    def verify_kafka_messages(self) -> Dict[str, int]:
        """Verify messages were sent to Kafka topics"""
        logger.info("\n" + "="*50)
        logger.info("Verifying Kafka Messages")
        logger.info("="*50)
        
        topics = ['acra-companies', 'singstat-economics', 'ura-geospatial']
        message_counts = {}
        
        for topic in topics:
            try:
                logger.info(f"Checking topic: {topic}")
                
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.kafka_config['bootstrap_servers'],
                    auto_offset_reset='earliest',
                    consumer_timeout_ms=10000,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                
                messages = []
                for message in consumer:
                    messages.append(message.value)
                    if len(messages) >= 100:  # Limit for testing
                        break
                
                consumer.close()
                
                message_counts[topic] = len(messages)
                logger.info(f"✅ Topic {topic}: {len(messages)} messages")
                
                # Log sample message
                if messages:
                    logger.info(f"Sample message from {topic}:")
                    logger.info(json.dumps(messages[0], indent=2, default=str)[:500] + "...")
                
            except Exception as e:
                logger.error(f"❌ Error checking topic {topic}: {e}")
                message_counts[topic] = 0
        
        return message_counts
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive test report"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        # Verify Kafka messages
        message_counts = self.verify_kafka_messages()
        
        report = {
            'test_summary': {
                'start_time': self.start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'overall_status': 'success' if all(r['status'] == 'success' for r in self.test_results.values()) else 'partial_success'
            },
            'data_sources': self.test_results,
            'kafka_verification': message_counts,
            'infrastructure_status': {
                'kafka_connectivity': True,  # We tested this earlier
                'topics_created': list(message_counts.keys()),
                'total_messages_sent': sum(message_counts.values())
            }
        }
        
        return report
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all data source tests"""
        logger.info("\n" + "="*60)
        logger.info("PHASE A: DATA SOURCE VERIFICATION TESTING")
        logger.info("Economic Intelligence Platform")
        logger.info("="*60)
        
        # Test Kafka connectivity first
        if not self.test_kafka_connectivity():
            logger.error("❌ Kafka connectivity failed - aborting tests")
            return {'error': 'Kafka connectivity failed'}
        
        # Test each data source
        logger.info("\nTesting all data sources...")
        
        # Test ACRA
        self.test_acra_producer()
        time.sleep(5)  # Brief pause between tests
        
        # Test SingStat
        self.test_singstat_producer()
        time.sleep(5)
        
        # Test URA
        self.test_ura_producer()
        time.sleep(5)
        
        # Generate final report
        report = self.generate_report()
        
        # Save report to file
        report_file = f"phase_a_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"\n📊 Test report saved to: {report_file}")
        
        # Print summary
        self.print_summary(report)
        
        return report
    
    def print_summary(self, report: Dict[str, Any]):
        """Print test summary"""
        logger.info("\n" + "="*60)
        logger.info("PHASE A TEST SUMMARY")
        logger.info("="*60)
        
        summary = report['test_summary']
        logger.info(f"Duration: {summary['duration_seconds']:.2f} seconds")
        logger.info(f"Overall Status: {summary['overall_status'].upper()}")
        
        logger.info("\nData Source Results:")
        for source, result in report['data_sources'].items():
            status_icon = "✅" if result['status'] == 'success' else "❌"
            logger.info(f"  {status_icon} {source.upper()}: {result['status']} ({result['records']} records)")
            if result['errors']:
                logger.info(f"    Errors: {len(result['errors'])}")
        
        logger.info("\nKafka Verification:")
        for topic, count in report['kafka_verification'].items():
            logger.info(f"  📨 {topic}: {count} messages")
        
        total_messages = report['infrastructure_status']['total_messages_sent']
        logger.info(f"\n📊 Total Messages Sent: {total_messages}")
        
        if summary['overall_status'] == 'success':
            logger.info("\n🎉 PHASE A COMPLETED SUCCESSFULLY!")
            logger.info("All data sources are working with the backend infrastructure.")
        else:
            logger.info("\n⚠️  PHASE A COMPLETED WITH ISSUES")
            logger.info("Some data sources may need attention.")


def main():
    """Main function"""
    tester = DataSourceTester()
    
    try:
        report = tester.run_all_tests()
        
        # Exit with appropriate code
        if report.get('test_summary', {}).get('overall_status') == 'success':
            sys.exit(0)
        else:
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("\n⏹️  Test interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n💥 Test failed with exception: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()