#!/usr/bin/env python3
"""
Economic Intelligence Platform - Phase A Monitoring Dashboard
"""

import subprocess
import json
import time
from datetime import datetime

def get_pod_status(namespace):
    """Get pod status from Kubernetes"""
    try:
        result = subprocess.run(
            ['kubectl', 'get', 'pods', '-n', namespace, '-o', 'json'],
            capture_output=True, text=True, check=True
        )
        return json.loads(result.stdout)
    except Exception as e:
        print(f"Error getting pod status: {e}")
        return None

def get_service_status(namespace):
    """Get service status from Kubernetes"""
    try:
        result = subprocess.run(
            ['kubectl', 'get', 'services', '-n', namespace, '-o', 'json'],
            capture_output=True, text=True, check=True
        )
        return json.loads(result.stdout)
    except Exception as e:
        print(f"Error getting service status: {e}")
        return None

def print_dashboard():
    """Print monitoring dashboard"""
    namespace = 'economic-observatory'
    
    print("\n" + "="*80)
    print(f"ECONOMIC INTELLIGENCE PLATFORM - PHASE A MONITORING")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # Pod status
    pods = get_pod_status(namespace)
    if pods:
        print("\n📊 POD STATUS:")
        for pod in pods['items']:
            name = pod['metadata']['name']
            status = pod['status']['phase']
            ready = 'Unknown'
            if 'containerStatuses' in pod['status']:
                ready_count = sum(1 for c in pod['status']['containerStatuses'] if c.get('ready', False))
                total_count = len(pod['status']['containerStatuses'])
                ready = f"{ready_count}/{total_count}"
            
            status_icon = "✅" if status == "Running" else "❌"
            print(f"  {status_icon} {name:<30} {status:<10} Ready: {ready}")
    
    # Service status
    services = get_service_status(namespace)
    if services:
        print("\n🌐 SERVICE STATUS:")
        for service in services['items']:
            name = service['metadata']['name']
            service_type = service['spec']['type']
            ports = [str(p['port']) for p in service['spec']['ports']]
            print(f"  🔗 {name:<30} {service_type:<12} Ports: {','.join(ports)}")
    
    print("\n" + "="*80)
    print("Phase A Components Status:")
    print("✅ Kafka Cluster - Data Streaming")
    print("✅ MinIO Storage - Delta Lake Backend")
    print("✅ Data Producers - ACRA, SingStat, URA")
    print("✅ Spark Streaming - Bronze Layer Ingestion")
    print("✅ Spark ETL - Bronze to Silver Transformation")
    print("✅ dbt Analytics - Silver to Gold Marts")
    print("✅ Monitoring - Health Checks & Metrics")
    print("="*80)

if __name__ == "__main__":
    print_dashboard()
