import boto3
import yaml 

s3 = boto3.client('s3')
BUCKET_NAME = "oubt-training-priya-2025"

def upload_manifest():
    # Define the location for the manifest
    # We put it in a 'mgmt/' prefix to keep it separate from lab data
    manifest_key = "day2/mgmt/bucket_manifest.yaml"
    local_file = "bucket-governance-manifest.yaml"

    try:
        # Upload the YAML file
        with open(local_file, 'rb') as data:
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=manifest_key,
                Body=data,
                ContentType='text/yaml'
            )
        print(f"✅ Manifest uploaded to s3://{BUCKET_NAME}/{manifest_key}")
    except Exception as e:
        print(f"❌ Failed to upload manifest: {e}")

if __name__ == "__main__":
    upload_manifest()