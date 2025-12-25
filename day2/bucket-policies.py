import boto3
import json

# Initialize the S3 client
s3 = boto3.client('s3')

# 1. Define Names
# Note: Bucket names must be globally unique. 
# If 'oubt-training' is taken, add your name/numbers (e.g., 'oubt-training-priya-2025')
BUCKET_NAME = "oubt-training-priya-2025" 
FOLDER_NAME = "day2"
FILE_NAME = "data.json"
OBJECT_KEY = f"{FOLDER_NAME}/{FILE_NAME}"

def run_lab():
    try:
        # STEP 1: Create the Bucket
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Bucket '{BUCKET_NAME}' created.")

        # STEP 2: Create a JSON file locally and upload it
        content = {"message": "Hello from Day 2!", "status": "Success"}
        
        # Upload directly to S3 with the specific Key (day2/data.json)
        s3.put_object(
            Bucket=BUCKET_NAME, 
            Key=OBJECT_KEY, 
            Body=json.dumps(content)
        )
        print(f"✅ File uploaded to Key: {OBJECT_KEY}")

        # STEP 3: Configure Bucket Policy
        bucket_arn = f"arn:aws:s3:::{BUCKET_NAME}"
        policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "AllowReadForDay2Folder",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"{bucket_arn}/{FOLDER_NAME}/*"
            }]
        }

        s3.put_bucket_policy(Bucket=BUCKET_NAME, Policy=json.dumps(policy))
        print(f"✅ Policy applied to path: {FOLDER_NAME}/*")

    except Exception as e:
        print(f"❌ Error occurred: {e}")

if __name__ == "__main__":
    run_lab()