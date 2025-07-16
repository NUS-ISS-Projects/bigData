#!/usr/bin/env python3
"""
Health Check System for Economic Intelligence Platform
Provides comprehensive health monitoring for all data sources and infrastructure components.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import os
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class HealthStatus:
    """Health status for a component"""
    component: str
    status: str  # 'healthy', 'degraded', 'unhealthy'
    message: str
    timestamp: datetime
    response_time_ms: Optional[float] = None
    details: Optional[Dict[str, Any]] = None

class HealthChecker:
    """Comprehensive health checker for the platform"""
    
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'value_serializer': lambda v: json.dumps(v).encode('utf-8')
        }
        self.topics = [
            'acra-companies',
            'singstat-economics', 
            'ura-geospatial'
        ]
        
    def check_kafka_connectivity(self) -> HealthStatus:
        """Check Kafka cluster connectivity"""
        start_time = time.time()
        try:
            producer = KafkaProducer(**self.kafka_config, request_timeout_ms=5000)
            producer.close()
            
            response_time = (time.time() - start_time) * 1000
            return HealthStatus(
                component='kafka',
                status='healthy',
                message='Kafka cluster is accessible',
                timestamp=datetime.now(),
                response_time_ms=response_time
            )
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthStatus(
                component='kafka',
                status='unhealthy',
                message=f'Kafka connectivity failed: {str(e)}',
                timestamp=datetime.now(),
                response_time_ms=response_time
            )
    
    def check_kafka_topics(self) -> List[HealthStatus]:
        """Check if required Kafka topics exist and are accessible"""
        results = []
        
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                consumer_timeout_ms=5000
            )
            
            available_topics = consumer.topics()
            consumer.close()
            
            for topic in self.topics:
                if topic in available_topics:
                    results.append(HealthStatus(
                        component=f'kafka-topic-{topic}',
                        status='healthy',
                        message=f'Topic {topic} is available',
                        timestamp=datetime.now()
                    ))
                else:
                    results.append(HealthStatus(
                        component=f'kafka-topic-{topic}',
                        status='unhealthy',
                        message=f'Topic {topic} is not available',
                        timestamp=datetime.now()
                    ))
                    
        except Exception as e:
            for topic in self.topics:
                results.append(HealthStatus(
                    component=f'kafka-topic-{topic}',
                    status='unhealthy',
                    message=f'Failed to check topic {topic}: {str(e)}',
                    timestamp=datetime.now()
                ))
        
        return results
    
    def check_acra_api(self) -> HealthStatus:
        """Check ACRA API health"""
        start_time = time.time()
        try:
            base_url = os.getenv('ACRA_BASE_URL', 'https://data.gov.sg/api/action')
            resource_id = os.getenv('ACRA_RESOURCE_ID', 'd_3f960c10fed6145404ca7b821f263b87')
            
            url = f"{base_url}/datastore_search"
            params = {
                'resource_id': resource_id,
                'limit': 1  # Minimal request for health check
            }
            
            response = requests.get(url, params=params, timeout=10)
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    return HealthStatus(
                        component='acra-api',
                        status='healthy',
                        message='ACRA API is responding correctly',
                        timestamp=datetime.now(),
                        response_time_ms=response_time,
                        details={'records_available': len(data.get('result', {}).get('records', []))}
                    )
                else:
                    return HealthStatus(
                        component='acra-api',
                        status='degraded',
                        message='ACRA API responded but with errors',
                        timestamp=datetime.now(),
                        response_time_ms=response_time
                    )
            else:
                return HealthStatus(
                    component='acra-api',
                    status='unhealthy',
                    message=f'ACRA API returned status {response.status_code}',
                    timestamp=datetime.now(),
                    response_time_ms=response_time
                )
                
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthStatus(
                component='acra-api',
                status='unhealthy',
                message=f'ACRA API check failed: {str(e)}',
                timestamp=datetime.now(),
                response_time_ms=response_time
            )
    
    def check_singstat_api(self) -> HealthStatus:
        """Check SingStat API health"""
        start_time = time.time()
        try:
            base_url = os.getenv('SINGSTAT_BASE_URL', 'https://tablebuilder.singstat.gov.sg/api/table')
            
            # Use a simple metadata endpoint for health check
            url = f"{base_url}/metadata/M015721"
            
            response = requests.get(url, timeout=10)
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                return HealthStatus(
                    component='singstat-api',
                    status='healthy',
                    message='SingStat API is responding correctly',
                    timestamp=datetime.now(),
                    response_time_ms=response_time
                )
            else:
                return HealthStatus(
                    component='singstat-api',
                    status='unhealthy',
                    message=f'SingStat API returned status {response.status_code}',
                    timestamp=datetime.now(),
                    response_time_ms=response_time
                )
                
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthStatus(
                component='singstat-api',
                status='unhealthy',
                message=f'SingStat API check failed: {str(e)}',
                timestamp=datetime.now(),
                response_time_ms=response_time
            )
    
    def check_ura_api(self) -> HealthStatus:
        """Check URA API health"""
        start_time = time.time()
        try:
            token_url = os.getenv('URA_TOKEN_URL', 'https://eservice.ura.gov.sg/uraDataService/insertNewToken.action')
            access_key = os.getenv('URA_ACCESS_KEY')
            
            if not access_key:
                return HealthStatus(
                    component='ura-api',
                    status='unhealthy',
                    message='URA Access Key not configured',
                    timestamp=datetime.now()
                )
            
            headers = {
                'AccessKey': access_key,
                'User-Agent': 'Economic Intelligence Platform Health Check'
            }
            
            response = requests.get(token_url, headers=headers, timeout=10)
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data.get('Status') == 'Success':
                        return HealthStatus(
                            component='ura-api',
                            status='healthy',
                            message='URA API is responding correctly',
                            timestamp=datetime.now(),
                            response_time_ms=response_time
                        )
                    else:
                        return HealthStatus(
                            component='ura-api',
                            status='degraded',
                            message=f'URA API responded but with status: {data.get("Status")}',
                            timestamp=datetime.now(),
                            response_time_ms=response_time
                        )
                except json.JSONDecodeError:
                    return HealthStatus(
                        component='ura-api',
                        status='degraded',
                        message='URA API responded but with invalid JSON',
                        timestamp=datetime.now(),
                        response_time_ms=response_time
                    )
            else:
                return HealthStatus(
                    component='ura-api',
                    status='unhealthy',
                    message=f'URA API returned status {response.status_code}',
                    timestamp=datetime.now(),
                    response_time_ms=response_time
                )
                
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthStatus(
                component='ura-api',
                status='unhealthy',
                message=f'URA API check failed: {str(e)}',
                timestamp=datetime.now(),
                response_time_ms=response_time
            )
    
    def run_comprehensive_health_check(self) -> Dict[str, Any]:
        """Run all health checks and return comprehensive status"""
        logger.info("Starting comprehensive health check...")
        
        all_checks = []
        
        # Infrastructure checks
        all_checks.append(self.check_kafka_connectivity())
        all_checks.extend(self.check_kafka_topics())
        
        # API checks
        all_checks.append(self.check_acra_api())
        all_checks.append(self.check_singstat_api())
        all_checks.append(self.check_ura_api())
        
        # Calculate overall status
        healthy_count = sum(1 for check in all_checks if check.status == 'healthy')
        degraded_count = sum(1 for check in all_checks if check.status == 'degraded')
        unhealthy_count = sum(1 for check in all_checks if check.status == 'unhealthy')
        
        if unhealthy_count > 0:
            overall_status = 'unhealthy'
        elif degraded_count > 0:
            overall_status = 'degraded'
        else:
            overall_status = 'healthy'
        
        # Prepare response
        response = {
            'overall_status': overall_status,
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_checks': len(all_checks),
                'healthy': healthy_count,
                'degraded': degraded_count,
                'unhealthy': unhealthy_count
            },
            'checks': [
                {
                    'component': check.component,
                    'status': check.status,
                    'message': check.message,
                    'timestamp': check.timestamp.isoformat(),
                    'response_time_ms': check.response_time_ms,
                    'details': check.details
                }
                for check in all_checks
            ]
        }
        
        logger.info(f"Health check completed. Overall status: {overall_status}")
        return response

def main():
    """Main function for running health checks"""
    checker = HealthChecker()
    result = checker.run_comprehensive_health_check()
    
    # Pretty print results
    print(json.dumps(result, indent=2, default=str))
    
    # Exit with appropriate code
    if result['overall_status'] == 'healthy':
        exit(0)
    elif result['overall_status'] == 'degraded':
        exit(1)
    else:
        exit(2)

if __name__ == '__main__':
    main()