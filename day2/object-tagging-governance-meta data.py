import boto3

s3 = boto3.client('s3')
BUCKET_NAME = "oubt-training-priya-2025"

def upload_with_governance_tags():
    file_key = "day2/finance/quarterly_report.json"
    content = '{"revenue": 50000, "status": "final"}'
    
    # 1. Define your Governance Metadata
    # Values: Classification (Public/Internal/Secret), Domain (Finance/HR/Eng)
    tags = "Owner=Priya&Classification=Internal&Domain=Finance"

    try:
        # 2. Upload with Tagging parameter
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_key,
            Body=content,
            Tagging=tags  # This attaches the metadata to the object
        )
        print(f"✅ Uploaded {file_key} with Governance Tags.")
        
    except Exception as e:
        print(f"❌ Tagging Error: {e}")

if __name__ == "__main__":
    upload_with_governance_tags()