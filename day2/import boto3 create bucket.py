import boto3
import json
from botocore.exceptions import ClientError

# --- SETTINGS ---
# IMPORTANT: This name must be unique across all of AWS! 
# Try adding your name and today's date (e.g., 'priya-data-bucket-2025')
BUCKET_NAME = 'test-bucket-check-boto3' 
REGION = 'us-east-1' # Change to your preferred region
FILE_NAME = 'cloud_config.json'

# Data to be saved
config_data = {
    "project": "Automation",
    "environment": "Development",
    "version": 1.0
}

def setup_s3():
    s3 = boto3.client('s3', region_name=REGION)

    # 1. CREATE THE BUCKET
    try:
        print(f"Creating bucket: {BUCKET_NAME}...")
        
        # Note: us-east-1 is the default; other regions require LocationConstraint
        if REGION == 'us-east-1':
            s3.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={'LocationConstraint': REGION}
            )
        print("✅ Bucket created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print("ℹ️ Bucket already exists and you own it.")
        else:
            print(f"❌ Error creating bucket: {e}")
            return

    # 2. CREATE LOCAL JSON FILE
    with open(FILE_NAME, 'w') as f:
        json.dump(config_data, f, indent=4)
    print(f"✅ Created local file: {FILE_NAME}")

    # 3. UPLOAD TO S3
    try:
        s3.upload_file(FILE_NAME, BUCKET_NAME, FILE_NAME)
        print(f"🚀 Successfully uploaded {FILE_NAME} to {BUCKET_NAME}!")
    except Exception as e:
        print(f"❌ Upload failed: {e}")

if __name__ == "__main__":
    setup_s3()