import boto3
import json
from datetime import datetime, timedelta
import os
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    VIJAY's S3 Bucket Cleanup Lambda Function
    Deletes files older than specified retention period
    """
    
    # Get configuration from environment variables or event
    bucket_name = os.environ.get('BUCKET_NAME', event.get('bucket_name'))
    retention_days = int(os.environ.get('RETENTION_DAYS', event.get('retention_days', 30)))
    dry_run = os.environ.get('DRY_RUN', str(event.get('dry_run', True))).lower() == 'true'
    
    logger.info(f"VIJAY's S3 Cleanup starting for bucket: {bucket_name}")
    logger.info(f"Retention days: {retention_days}, Dry run: {dry_run}")
    
    if not bucket_name:
        return {
            'statusCode': 400,
            'body': json.dumps('Error: BUCKET_NAME not specified')
        }
    
    s3_client = boto3.client('s3')
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    
    deleted_files = []
    total_size_deleted = 0
    error_count = 0
    
    try:
        # List all objects in the bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                file_key = obj['Key']
                last_modified = obj['LastModified'].replace(tzinfo=None)
                file_size = obj['Size']
                
                # Check if file is older than cutoff date
                if last_modified < cutoff_date:
                    if dry_run:
                        logger.info(f"VIJAY - Would delete: {file_key} (modified: {last_modified})")
                        deleted_files.append(file_key)
                        total_size_deleted += file_size
                    else:
                        try:
                            s3_client.delete_object(Bucket=bucket_name, Key=file_key)
                            logger.info(f"VIJAY - Deleted: {file_key} (modified: {last_modified})")
                            deleted_files.append(file_key)
                            total_size_deleted += file_size
                        except Exception as e:
                            logger.error(f"VIJAY - Error deleting {file_key}: {str(e)}")
                            error_count += 1
    
    except Exception as e:
        logger.error(f"VIJAY - Error processing bucket {bucket_name}: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
    
    # Prepare response
    result = {
        'bucket_name': bucket_name,
        'retention_days': retention_days,
        'dry_run': dry_run,
        'files_processed': len(deleted_files),
        'total_size_mb': round(total_size_deleted / (1024*1024), 2),
        'error_count': error_count,
        'cutoff_date': cutoff_date.isoformat()
    }
    
    logger.info(f"VIJAY - Cleanup completed: {json.dumps(result)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }