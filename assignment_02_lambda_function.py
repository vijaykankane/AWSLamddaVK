import boto3
import json
import logging
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda function to automatically delete files older than 30 days from S3 bucket.
    """
    
    # Configuration - can be set via environment variables
    BUCKET_NAME = os.environ.get('BUCKET_NAME', event.get('bucket_name', ''))
    RETENTION_DAYS = int(os.environ.get('RETENTION_DAYS', event.get('retention_days', 30)))
    DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() == 'true'
    
    if not BUCKET_NAME:
        return {
            'statusCode': 400,
            'body': json.dumps('Error: BUCKET_NAME must be provided via environment variable or event')
        }
    
    # Initialize S3 client
    try:
        s3_client = boto3.client('s3')
        logger.info(f"Successfully initialized S3 client for bucket: {BUCKET_NAME}")
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error initializing S3 client: {str(e)}')
        }
    
    results = {
        'bucket_name': BUCKET_NAME,
        'retention_days': RETENTION_DAYS,
        'dry_run': DRY_RUN,
        'files_processed': 0,
        'files_deleted': 0,
        'total_size_deleted': 0,
        'deleted_files': [],
        'errors': []
    }
    
    try:
        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=BUCKET_NAME)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return {
                    'statusCode': 404,
                    'body': json.dumps(f'Bucket {BUCKET_NAME} does not exist')
                }
            else:
                raise e
        
        # Calculate cutoff date
        current_time = datetime.now(timezone.utc)
        cutoff_date = current_time.timestamp() - (RETENTION_DAYS * 24 * 60 * 60)
        
        logger.info(f"Deleting files older than {RETENTION_DAYS} days (before {datetime.fromtimestamp(cutoff_date, timezone.utc)})")
        
        # List and process objects in bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=BUCKET_NAME)
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    results['files_processed'] += 1
                    
                    # Get object details
                    object_key = obj['Key']
                    last_modified = obj['LastModified'].timestamp()
                    file_size = obj['Size']
                    
                    # Check if file is older than retention period
                    if last_modified < cutoff_date:
                        file_age_days = (current_time.timestamp() - last_modified) / (24 * 60 * 60)
                        
                        if DRY_RUN:
                            logger.info(f"DRY RUN: Would delete {object_key} (age: {file_age_days:.1f} days, size: {file_size} bytes)")
                            results['deleted_files'].append({
                                'key': object_key,
                                'age_days': round(file_age_days, 1),
                                'size_bytes': file_size,
                                'last_modified': obj['LastModified'].isoformat(),
                                'action': 'would_delete'
                            })
                        else:
                            try:
                                # Delete the object
                                s3_client.delete_object(Bucket=BUCKET_NAME, Key=object_key)
                                
                                results['files_deleted'] += 1
                                results['total_size_deleted'] += file_size
                                
                                results['deleted_files'].append({
                                    'key': object_key,
                                    'age_days': round(file_age_days, 1),
                                    'size_bytes': file_size,
                                    'last_modified': obj['LastModified'].isoformat(),
                                    'action': 'deleted'
                                })
                                
                                logger.info(f"Deleted: {object_key} (age: {file_age_days:.1f} days, size: {file_size} bytes)")
                                
                            except ClientError as e:
                                error_msg = f"Failed to delete {object_key}: {str(e)}"
                                logger.error(error_msg)
                                results['errors'].append(error_msg)
                    else:
                        # File is within retention period
                        file_age_days = (current_time.timestamp() - last_modified) / (24 * 60 * 60)
                        logger.debug(f"Keeping: {object_key} (age: {file_age_days:.1f} days)")
        
        # Log summary
        if DRY_RUN:
            logger.info(f"DRY RUN completed. Processed: {results['files_processed']} files, "
                       f"Would delete: {len(results['deleted_files'])} files")
        else:
            logger.info(f"Cleanup completed. Processed: {results['files_processed']} files, "
                       f"Deleted: {results['files_deleted']} files, "
                       f"Total size deleted: {results['total_size_deleted']} bytes, "
                       f"Errors: {len(results['errors'])}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'S3 bucket cleanup completed successfully',
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

def format_file_size(size_bytes):
    """Convert bytes to human readable format"""
    if size_bytes == 0:
        return "0B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_names[i]}"