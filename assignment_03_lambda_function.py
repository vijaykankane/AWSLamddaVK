import boto3
import json
import logging
from botocore.exceptions import ClientError, NoCredentialsError
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda function to detect S3 buckets without server-side encryption enabled.
    Checks all buckets for encryption configuration and reports unencrypted ones.
    """
    
    # Configuration
    SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')
    INCLUDE_PUBLIC_READ_CHECK = os.environ.get('INCLUDE_PUBLIC_READ_CHECK', 'true').lower() == 'true'
    
    # Initialize AWS clients
    try:
        s3_client = boto3.client('s3')
        logger.info("Successfully initialized S3 client")
        
        sns_client = None
        if SNS_TOPIC_ARN:
            sns_client = boto3.client('sns')
            logger.info("Successfully initialized SNS client")
            
    except NoCredentialsError:
        logger.error("AWS credentials not found")
        return {
            'statusCode': 500,
            'body': json.dumps('Error: AWS credentials not configured')
        }
    except Exception as e:
        logger.error(f"Failed to initialize AWS clients: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error initializing AWS clients: {str(e)}')
        }
    
    results = {
        'total_buckets': 0,
        'encrypted_buckets': 0,
        'unencrypted_buckets': 0,
        'inaccessible_buckets': 0,
        'unencrypted_bucket_details': [],
        'public_buckets': [],
        'errors': []
    }
    
    try:
        # List all S3 buckets
        response = s3_client.list_buckets()
        buckets = response.get('Buckets', [])
        results['total_buckets'] = len(buckets)
        
        logger.info(f"Found {len(buckets)} S3 buckets to analyze")
        
        for bucket in buckets:
            bucket_name = bucket['Name']
            bucket_creation_date = bucket['CreationDate']
            
            logger.info(f"Analyzing bucket: {bucket_name}")
            
            bucket_details = {
                'name': bucket_name,
                'creation_date': bucket_creation_date.isoformat(),
                'encryption_status': 'unknown',
                'encryption_rules': [],
                'public_read_access': False,
                'public_write_access': False,
                'location': 'unknown'
            }
            
            try:
                # Get bucket location
                try:
                    location_response = s3_client.get_bucket_location(Bucket=bucket_name)
                    bucket_details['location'] = location_response.get('LocationConstraint', 'us-east-1')
                except ClientError:
                    bucket_details['location'] = 'unknown'
                
                # Check bucket encryption configuration
                encryption_status = check_bucket_encryption(s3_client, bucket_name)
                bucket_details.update(encryption_status)
                
                if bucket_details['encryption_status'] == 'encrypted':
                    results['encrypted_buckets'] += 1
                    logger.info(f"Bucket {bucket_name} is encrypted")
                else:
                    results['unencrypted_buckets'] += 1
                    results['unencrypted_bucket_details'].append(bucket_details)
                    logger.warning(f"Bucket {bucket_name} is NOT encrypted")
                
                # Check public access if enabled
                if INCLUDE_PUBLIC_READ_CHECK:
                    public_access = check_bucket_public_access(s3_client, bucket_name)
                    bucket_details.update(public_access)
                    
                    if bucket_details['public_read_access'] or bucket_details['public_write_access']:
                        results['public_buckets'].append(bucket_details)
                        logger.warning(f"Bucket {bucket_name} has public access")
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ['AccessDenied', 'NoSuchBucket']:
                    results['inaccessible_buckets'] += 1
                    error_msg = f"Cannot access bucket {bucket_name}: {error_code}"
                    logger.warning(error_msg)
                    results['errors'].append(error_msg)
                else:
                    error_msg = f"Error analyzing bucket {bucket_name}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
            except Exception as e:
                error_msg = f"Unexpected error analyzing bucket {bucket_name}: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
        
        # Generate summary report
        summary = generate_summary_report(results)
        logger.info(summary)
        
        # Send SNS notification if configured and unencrypted buckets found
        if sns_client and SNS_TOPIC_ARN and results['unencrypted_buckets'] > 0:
            send_sns_notification(sns_client, SNS_TOPIC_ARN, results, summary)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'S3 encryption audit completed successfully',
                'summary': summary,
                'results': results
            }, indent=2, default=str)
        }
        
    except ClientError as e:
        error_msg = f"AWS API Error: {str(e)}"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_msg,
                'results': results
            })
        }
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_msg,
                'results': results
            })
        }

def check_bucket_encryption(s3_client, bucket_name):
    """
    Check if a bucket has server-side encryption enabled.
    Returns encryption status and configuration details.
    """
    encryption_details = {
        'encryption_status': 'unencrypted',
        'encryption_rules': []
    }
    
    try:
        response = s3_client.get_bucket_encryption(Bucket=bucket_name)
        
        if 'ServerSideEncryptionConfiguration' in response:
            encryption_details['encryption_status'] = 'encrypted'
            
            rules = response['ServerSideEncryptionConfiguration'].get('Rules', [])
            for rule in rules:
                sse_algorithm = rule.get('ApplyServerSideEncryptionByDefault', {}).get('SSEAlgorithm', 'unknown')
                kms_key_id = rule.get('ApplyServerSideEncryptionByDefault', {}).get('KMSMasterKeyID', '')
                
                rule_details = {
                    'algorithm': sse_algorithm,
                    'kms_key_id': kms_key_id,
                    'bucket_key_enabled': rule.get('BucketKeyEnabled', False)
                }
                encryption_details['encryption_rules'].append(rule_details)
                
                logger.info(f"Bucket {bucket_name} has {sse_algorithm} encryption")
    
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ServerSideEncryptionConfigurationNotFoundError':
            # This is expected for unencrypted buckets
            logger.info(f"Bucket {bucket_name} has no encryption configuration")
        else:
            logger.error(f"Error checking encryption for bucket {bucket_name}: {str(e)}")
            encryption_details['encryption_status'] = 'error'
    
    return encryption_details

def check_bucket_public_access(s3_client, bucket_name):
    """
    Check if a bucket has public read or write access.
    """
    public_access_details = {
        'public_read_access': False,
        'public_write_access': False,
        'public_access_block': {}
    }
    
    try:
        # Check Public Access Block configuration
        try:
            response = s3_client.get_public_access_block(Bucket=bucket_name)
            public_access_details['public_access_block'] = response.get('PublicAccessBlockConfiguration', {})
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchPublicAccessBlockConfiguration':
                logger.warning(f"Could not get public access block for {bucket_name}: {str(e)}")
        
        # Check bucket ACL for public access
        try:
            acl_response = s3_client.get_bucket_acl(Bucket=bucket_name)
            for grant in acl_response.get('Grants', []):
                grantee = grant.get('Grantee', {})
                permission = grant.get('Permission', '')
                
                # Check for public read access
                if (grantee.get('Type') == 'Group' and 
                    grantee.get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers' and
                    permission in ['READ', 'FULL_CONTROL']):
                    public_access_details['public_read_access'] = True
                
                # Check for public write access
                if (grantee.get('Type') == 'Group' and 
                    grantee.get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers' and
                    permission in ['WRITE', 'FULL_CONTROL']):
                    public_access_details['public_write_access'] = True
                    
        except ClientError as e:
            logger.warning(f"Could not check ACL for bucket {bucket_name}: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error checking public access for bucket {bucket_name}: {str(e)}")
    
    return public_access_details

def generate_summary_report(results):
    """Generate a human-readable summary of the audit results."""
    summary = f"""
S3 Encryption Audit Summary:
============================
Total Buckets: {results['total_buckets']}
Encrypted Buckets: {results['encrypted_buckets']}
Unencrypted Buckets: {results['unencrypted_buckets']}
Inaccessible Buckets: {results['inaccessible_buckets']}
Public Buckets: {len(results['public_buckets'])}
Errors: {len(results['errors'])}

Unencrypted Buckets:
"""
    
    if results['unencrypted_bucket_details']:
        for bucket in results['unencrypted_bucket_details']:
            summary += f"- {bucket['name']} (created: {bucket['creation_date'][:10]})\n"
    else:
        summary += "None found - All buckets are encrypted!\n"
    
    if results['public_buckets']:
        summary += "\nPublic Buckets:\n"
        for bucket in results['public_buckets']:
            access_type = []
            if bucket['public_read_access']:
                access_type.append('READ')
            if bucket['public_write_access']:
                access_type.append('WRITE')
            summary += f"- {bucket['name']} ({', '.join(access_type)} access)\n"
    
    return summary.strip()

def send_sns_notification(sns_client, topic_arn, results, summary):
    """Send SNS notification about unencrypted buckets."""
    try:
        message = {
            'alert_type': 'S3_ENCRYPTION_AUDIT',
            'severity': 'HIGH' if results['unencrypted_buckets'] > 0 else 'LOW',
            'summary': summary,
            'unencrypted_count': results['unencrypted_buckets'],
            'unencrypted_buckets': [bucket['name'] for bucket in results['unencrypted_bucket_details']],
            'public_buckets': [bucket['name'] for bucket in results['public_buckets']]
        }
        
        sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message, indent=2),
            Subject=f'S3 Security Alert: {results["unencrypted_buckets"]} Unencrypted Buckets Found'
        )
        
        logger.info(f"SNS notification sent to {topic_arn}")
        
    except Exception as e:
        logger.error(f"Failed to send SNS notification: {str(e)}")