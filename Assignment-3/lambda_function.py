import boto3
import json
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    """
    Lambda function to detect S3 buckets without server-side encryption enabled.
    Checks all buckets for encryption configuration and reports unencrypted ones.
    """
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    unencrypted_buckets = []
    
    try:
        # List all S3 buckets
        response = s3_client.list_buckets()
        buckets = response.get('Buckets', [])
        
        print(f"Total buckets found: {len(buckets)}")
        
        for bucket in buckets:
            bucket_name = bucket['Name']
            print(f"Checking encryption for bucket: {bucket_name}")
            
            try:
                # Check if bucket has server-side encryption configured
                encryption_response = s3_client.get_bucket_encryption(Bucket=bucket_name)
                print(f"  ✓ Bucket '{bucket_name}' has encryption enabled")
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ServerSideEncryptionConfigurationNotFoundError':
                    # No encryption configuration found
                    unencrypted_buckets.append(bucket_name)
                    print(f"  ⚠️ Bucket '{bucket_name}' does NOT have encryption enabled")
                else:
                    # Other error (e.g., access denied)
                    print(f"  ❌ Error checking bucket '{bucket_name}': {e}")
    
    except Exception as e:
        print(f"Error listing buckets: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
    
    # Summary report
    print(f"\n=== ENCRYPTION MONITORING REPORT ===")
    print(f"Total buckets checked: {len(buckets)}")
    print(f"Unencrypted buckets found: {len(unencrypted_buckets)}")
    
    if unencrypted_buckets:
        print("⚠️ UNENCRYPTED BUCKETS:")
        for bucket in unencrypted_buckets:
            print(f"  - {bucket}")
    else:
        print("✓ All buckets have encryption enabled!")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'total_buckets': len(buckets),
            'unencrypted_buckets': unencrypted_buckets,
            'unencrypted_count': len(unencrypted_buckets)
        })
    }

# For local testing on Windows
if __name__ == "__main__":
    # Test the function locally
    result = lambda_handler({}, {})
    print(f"Function result: {result}")