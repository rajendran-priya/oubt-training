import boto3
import json

def lambda_handler(event, context):
   
    bucket_name = "day11-step-functions"
    prefix = "transformed-data/"  # The folder your Glue Job writes to
    
    s3_client = boto3.client('s3')
    
    try:
        # 2. List objects in the specified folder
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        # 3. Check if any files (Keys) exist in that folder
        if 'Contents' in response and len(response['Contents']) > 0:
            status = "SUCCESS"
            message = f"Found {len(response['Contents'])} files in {prefix}"
        else:
            status = "FAILED"
            message = f"No files found in {prefix}"
            
        return {
            'statusCode': 200,
            'body': {
                'status': status,
                'message': message
            }
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': {
                'status': "ERROR",
                'message': str(e)
            }
        }