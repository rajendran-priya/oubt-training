import boto3
from datetime import datetime

s3 = boto3.client('s3')
BUCKET_NAME = "oubt-training-priya-2025"

def upload_organized_data():
    # We will upload 3 files into different "folders" (prefixes)
    files_to_upload = {
        "day2/images/profile.jpg": "Fake Image Data",
        "day2/logs/2025/daily.log": "System is healthy",
        "day2/temp/old_cache.tmp": "This file should be deleted soon"
    }

    for key, content in files_to_upload.items():
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=content)
        print(f"✅ Uploaded to: {key}")

upload_organized_data()