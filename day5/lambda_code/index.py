import json

def lambda_handler(event, context):
    # This logs the name of the file uploaded to S3
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    print(f"File uploaded! Bucket: {bucket}, Key: {key}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('S3 Trigger Processed Successfully!')
    }
