from silver_data_connector import SilverLayerConnector
import pandas as pd

conn = SilverLayerConnector()
acra_data = conn.load_acra_companies(limit=1000)

print('Data shape:', acra_data.shape)
print('Columns:', acra_data.columns.tolist())
print('Entity status column exists:', 'entity_status' in acra_data.columns)

if 'entity_status' in acra_data.columns:
    status_counts = acra_data['entity_status'].value_counts()
    print('Status counts:', status_counts)
    print('Status counts index:', status_counts.index.tolist())
    print('Status counts values:', status_counts.values.tolist())
    print('Length of status_counts:', len(status_counts))
    
    # Test the chart creation logic
    if len(status_counts) > 0:
        chart_data = {
            'type': 'donut',
            'data': {
                'labels': status_counts.index.tolist(),
                'values': status_counts.values.tolist()
            },
            'title': 'Company Status Distribution',
            'description': 'Current operational status of registered companies'
        }
        print('Chart data created successfully:', chart_data)
    else:
        print('No status data found!')

conn.close_connection()