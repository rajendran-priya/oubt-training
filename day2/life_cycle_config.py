import boto3
from datetime import datetime

s3 = boto3.client('s3')
BUCKET_NAME = "oubt-training-priya-2025"

def set_lifecycle_configuration():
    lifecycle_config = {
        'Rules': [
            {
                'ID': 'DeleteTempFilesRule',
                'Status': 'Enabled',
                'Filter': {'Prefix': 'day2/temp/'}, # Only targets the temp folder
                'Expiration': {'Days': 30}       # Deletes after 30 days
            }
        ]
    }

    try:
        s3.put_bucket_lifecycle_configuration(
            Bucket=BUCKET_NAME,
            LifecycleConfiguration=lifecycle_config
        )
        print("✅ Lifecycle rule set: 'day2/temp/' files will expire in 30 days.")
    except Exception as e:
        print(f"❌ Lifecycle Error: {e}")

set_lifecycle_configuration()