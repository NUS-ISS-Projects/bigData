#!/usr/bin/env python3
"""
MinIO Bucket Management Utility
Provides easy bucket management for the Economic Intelligence Platform
"""

import sys
import os
sys.path.append('scripts')

from init_minio_buckets import MinIOBucketInitializer
import argparse

def main():
    parser = argparse.ArgumentParser(description='Manage MinIO buckets')
    parser.add_argument('action', choices=['list', 'create', 'verify', 'recreate'], 
                       help='Action to perform')
    parser.add_argument('--endpoint', default='localhost:9000', help='MinIO endpoint')
    parser.add_argument('--access-key', default='admin', help='Access key')
    parser.add_argument('--secret-key', default='password123', help='Secret key')
    
    args = parser.parse_args()
    
    initializer = MinIOBucketInitializer(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key
    )
    
    if not initializer._create_client():
        print("❌ Failed to connect to MinIO")
        sys.exit(1)
    
    if args.action == 'list':
        initializer.list_buckets()
    elif args.action == 'create':
        initializer.create_all_buckets()
    elif args.action == 'verify':
        if initializer.verify_setup():
            print("✅ All buckets verified")
        else:
            print("❌ Bucket verification failed")
            sys.exit(1)
    elif args.action == 'recreate':
        print("🔄 Recreating all buckets...")
        initializer.create_all_buckets()
        initializer.verify_setup()

if __name__ == '__main__':
    main()
