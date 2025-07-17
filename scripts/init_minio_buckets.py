#!/usr/bin/env python3
"""
MinIO Bucket Initialization Script
Creates required buckets for the Economic Intelligence Platform using MinIO API
"""

import os
import sys
import time
import json
import logging
from minio import Minio
from minio.error import S3Error
import requests
from requests.exceptions import ConnectionError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MinIOBucketInitializer:
    """Initialize MinIO buckets for the Economic Intelligence Platform"""
    
    def __init__(self, endpoint=None, access_key=None, secret_key=None, secure=False, verbose=False):
        """
        Initialize MinIO client
        
        Args:
            endpoint: MinIO server endpoint (default: localhost:9000)
            access_key: MinIO access key (default: admin)
            secret_key: MinIO secret key (default: password123)
            secure: Use HTTPS (default: False)
            verbose: Enable verbose logging (default: False)
        """
        self.endpoint = endpoint or os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.access_key = access_key or os.getenv('MINIO_ACCESS_KEY', 'admin')
        self.secret_key = secret_key or os.getenv('MINIO_SECRET_KEY', 'password123')
        self.secure = secure
        self.verbose = verbose
        
        # Required buckets for the platform
        self.required_buckets = [
            'bronze',      # Raw data from Kafka
            'silver',      # Cleaned and transformed data
            'gold',        # Business-ready analytics data
            'checkpoints', # Spark streaming checkpoints
            'logs',        # Application logs
            'backups'      # Data backups
        ]
        
        self.client = None
        self.is_initialized = False
        
    def _create_client(self):
        """Create MinIO client with retry logic"""
        try:
            self.client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure
            )
            logger.info(f"MinIO client created for endpoint: {self.endpoint}")
            return True
        except Exception as e:
            logger.error(f"Failed to create MinIO client: {e}")
            return False
    
    def wait_for_minio(self, max_retries=30, retry_interval=10):
        """Wait for MinIO to be available"""
        logger.info("Waiting for MinIO to be available...")
        
        for attempt in range(max_retries):
            try:
                # Try to connect using HTTP request first
                url = f"http://{self.endpoint}/minio/health/live"
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    logger.info("MinIO health check passed")
                    break
            except (ConnectionError, requests.exceptions.Timeout):
                pass
            
            # Try creating client and listing buckets
            if self._create_client():
                try:
                    list(self.client.list_buckets())
                    logger.info("MinIO is available and accessible")
                    self.is_initialized = True
                    return True
                except Exception as e:
                    if self.verbose:
                        logger.debug(f"MinIO not ready yet: {e}")
            
            if attempt < max_retries - 1:
                logger.info(f"MinIO not ready, retrying in {retry_interval}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_interval)
            else:
                logger.error("MinIO failed to become available within timeout")
                return False
        
        # Final attempt to create client if health check passed
        if not self.is_initialized:
            if self._create_client():
                try:
                    list(self.client.list_buckets())
                    self.is_initialized = True
                    return True
                except Exception as e:
                    logger.error(f"Failed to initialize MinIO client: {e}")
                    return False
        
        return self.is_initialized
    
    def create_bucket(self, bucket_name):
        """Create a single bucket with error handling"""
        try:
            # Check if bucket already exists
            if self.client.bucket_exists(bucket_name):
                logger.info(f"Bucket '{bucket_name}' already exists")
                return True
            
            # Create the bucket
            self.client.make_bucket(bucket_name)
            logger.info(f"Successfully created bucket '{bucket_name}'")
            
            # Set public read policy for data buckets (bronze, silver, gold)
            if bucket_name in ['bronze', 'silver', 'gold']:
                self._set_bucket_policy(bucket_name)
            
            return True
            
        except S3Error as e:
            if e.code == 'BucketAlreadyOwnedByYou':
                logger.info(f"Bucket '{bucket_name}' already exists and is owned by you")
                return True
            else:
                logger.error(f"Failed to create bucket '{bucket_name}': {e}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error creating bucket '{bucket_name}': {e}")
            return False
    
    def _set_bucket_policy(self, bucket_name):
        """Set bucket policy for public read access"""
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"]
                },
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": ["s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}"]
                }
            ]
        }
        
        try:
            self.client.set_bucket_policy(bucket_name, json.dumps(policy))
            logger.info(f"Set public read policy for bucket '{bucket_name}'")
        except Exception as e:
            logger.warning(f"Failed to set policy for bucket '{bucket_name}': {e}")
    
    def create_all_buckets(self):
        """Create all required buckets"""
        # Ensure client is initialized
        if not self.is_initialized:
            if not self.wait_for_minio():
                logger.error("Failed to initialize MinIO client")
                return False
        
        logger.info("Creating all required buckets...")
        
        success_count = 0
        for bucket_name in self.required_buckets:
            if self.create_bucket(bucket_name):
                success_count += 1
        
        logger.info(f"Successfully created/verified {success_count}/{len(self.required_buckets)} buckets")
        return success_count == len(self.required_buckets)
    
    def list_buckets(self):
        """List all buckets"""
        # Ensure client is initialized
        if not self.is_initialized:
            if not self.wait_for_minio():
                logger.error("Failed to initialize MinIO client")
                return False
        
        try:
            buckets = self.client.list_buckets()
            logger.info("Current buckets:")
            for bucket in buckets:
                logger.info(f"  - {bucket.name} (created: {bucket.creation_date})")
            return True
        except Exception as e:
            logger.error(f"Failed to list buckets: {e}")
            return False
    
    def verify_setup(self):
        """Verify that all required buckets exist"""
        # Ensure client is initialized
        if not self.is_initialized:
            if not self.wait_for_minio():
                logger.error("Failed to initialize MinIO client")
                return False
        
        logger.info("Verifying bucket setup...")
        
        missing_buckets = []
        for bucket_name in self.required_buckets:
            try:
                if not self.client.bucket_exists(bucket_name):
                    missing_buckets.append(bucket_name)
            except Exception as e:
                logger.error(f"Error checking bucket '{bucket_name}': {e}")
                missing_buckets.append(bucket_name)
        
        if missing_buckets:
            logger.error(f"Missing buckets: {missing_buckets}")
            return False
        else:
            logger.info("All required buckets are present")
            return True
    
    def initialize(self):
        """Complete initialization process"""
        logger.info("Starting MinIO bucket initialization...")
        
        # Wait for MinIO to be available
        if not self.wait_for_minio():
            logger.error("MinIO is not available")
            return False
        
        # Create all buckets
        if not self.create_all_buckets():
            logger.error("Failed to create all required buckets")
            return False
        
        # List buckets for verification
        self.list_buckets()
        
        # Final verification
        if self.verify_setup():
            logger.info("✅ MinIO bucket initialization completed successfully")
            return True
        else:
            logger.error("❌ MinIO bucket initialization failed")
            return False

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Initialize MinIO buckets for Economic Intelligence Platform')
    parser.add_argument('--endpoint', default='localhost:9000', help='MinIO endpoint')
    parser.add_argument('--access-key', default='admin', help='MinIO access key')
    parser.add_argument('--secret-key', default='password123', help='MinIO secret key')
    parser.add_argument('--secure', action='store_true', help='Use HTTPS')
    parser.add_argument('--max-retries', type=int, default=30, help='Maximum retry attempts')
    parser.add_argument('--retry-interval', type=int, default=10, help='Retry interval in seconds')
    
    args = parser.parse_args()
    
    # Create initializer
    initializer = MinIOBucketInitializer(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        secure=args.secure
    )
    
    # Run initialization
    success = initializer.initialize()
    
    if success:
        logger.info("🎉 MinIO initialization completed successfully!")
        sys.exit(0)
    else:
        logger.error("💥 MinIO initialization failed!")
        sys.exit(1)

if __name__ == '__main__':
    main()