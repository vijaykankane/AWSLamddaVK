import boto3
import json
import logging
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda function to automatically create EBS snapshots and cleanup old ones.
    Creates snapshots for specified volumes and deletes snapshots older than retention period.
    """
    
    # Configuration from environment variables or event
    VOLUME_IDS = os.environ.get('VOLUME_IDS', event.get('volume_ids', '')).split(',')
    RETENTION_DAYS = int(os.environ.get('RETENTION_DAYS', event.get('retention_days', 30)))
    SNAPSHOT_DESCRIPTION_PREFIX = os.environ.get('SNAPSHOT_DESCRIPTION_PREFIX', 'AutoSnapshot')
    DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() == 'true'
    SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Remove empty volume IDs
    VOLUME_IDS = [vol_id.strip() for vol_id in VOLUME_IDS if vol_id.strip()]
    
    if not VOLUME_IDS:
        return {
            'statusCode': 400,
            'body': json.dumps('Error: No volume IDs specified. Set VOLUME_IDS environment variable or provide volume_ids in event.')
        }
    
    # Initialize AWS clients
    try:
        ec2_client = boto3.client('ec2')
        logger.info("Successfully initialized EC2 client")
        
        sns_client = None
        if SNS_TOPIC_ARN:
            sns_client = boto3.client('sns')
            logger.info("Successfully initialized SNS client")
            
    except Exception as e:
        logger.error(f"Failed to initialize AWS clients: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error initializing AWS clients: {str(e)}')
        }
    
    results = {
        'volume_ids_requested': VOLUME_IDS,
        'retention_days': RETENTION_DAYS,
        'dry_run': DRY_RUN,
        'snapshots_created': [],
        'snapshots_deleted': [],
        'volumes_processed': 0,
        'total_snapshots_cleaned': 0,
        'errors': []
    }
    
    try:
        # Create snapshots for specified volumes
        logger.info(f"Creating snapshots for volumes: {VOLUME_IDS}")
        for volume_id in VOLUME_IDS:
            try:
                snapshot_result = create_volume_snapshot(ec2_client, volume_id, SNAPSHOT_DESCRIPTION_PREFIX, DRY_RUN)
                if snapshot_result:
                    results['snapshots_created'].append(snapshot_result)
                    results['volumes_processed'] += 1
            except Exception as e:
                error_msg = f"Error creating snapshot for volume {volume_id}: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
        
        # Cleanup old snapshots
        logger.info(f"Cleaning up snapshots older than {RETENTION_DAYS} days")
        cleanup_result = cleanup_old_snapshots(ec2_client, RETENTION_DAYS, SNAPSHOT_DESCRIPTION_PREFIX, DRY_RUN)
        results['snapshots_deleted'] = cleanup_result['deleted_snapshots']
        results['total_snapshots_cleaned'] = len(cleanup_result['deleted_snapshots'])
        results['errors'].extend(cleanup_result['errors'])
        
        # Generate summary
        summary = generate_summary_report(results)
        logger.info(summary)
        
        # Send SNS notification if configured
        if sns_client and SNS_TOPIC_ARN:
            send_sns_notification(sns_client, SNS_TOPIC_ARN, results, summary)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'EBS snapshot management completed successfully',
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

def create_volume_snapshot(ec2_client, volume_id, description_prefix, dry_run=False):
    """
    Create a snapshot for a specific EBS volume.
    """
    try:
        # First, verify the volume exists and get its information
        try:
            volume_response = ec2_client.describe_volumes(VolumeIds=[volume_id])
            if not volume_response['Volumes']:
                raise Exception(f"Volume {volume_id} not found")
            
            volume = volume_response['Volumes'][0]
            volume_size = volume['Size']
            volume_type = volume['VolumeType']
            availability_zone = volume['AvailabilityZone']
            
            # Get instance information if volume is attached
            instance_id = 'unattached'
            device_name = 'N/A'
            if volume['Attachments']:
                instance_id = volume['Attachments'][0]['InstanceId']
                device_name = volume['Attachments'][0]['Device']
                
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidVolume.NotFound':
                raise Exception(f"Volume {volume_id} does not exist")
            else:
                raise e
        
        # Create snapshot description with metadata
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
        description = f"{description_prefix}-{volume_id}-{timestamp}"
        
        if dry_run:
            logger.info(f"DRY RUN: Would create snapshot for volume {volume_id}")
            return {
                'volume_id': volume_id,
                'snapshot_id': 'dry-run-snapshot-id',
                'description': description,
                'start_time': datetime.now(timezone.utc).isoformat(),
                'volume_size': volume_size,
                'volume_type': volume_type,
                'availability_zone': availability_zone,
                'instance_id': instance_id,
                'device_name': device_name,
                'action': 'would_create'
            }
        else:
            # Create the snapshot
            response = ec2_client.create_snapshot(
                VolumeId=volume_id,
                Description=description
            )
            
            snapshot_id = response['SnapshotId']
            
            # Add tags to the snapshot
            try:
                tags = [
                    {'Key': 'Name', 'Value': f"AutoSnapshot-{volume_id}"},
                    {'Key': 'CreatedBy', 'Value': 'Lambda-AutoSnapshot'},
                    {'Key': 'VolumeId', 'Value': volume_id},
                    {'Key': 'InstanceId', 'Value': instance_id},
                    {'Key': 'CreationDate', 'Value': timestamp}
                ]
                
                ec2_client.create_tags(
                    Resources=[snapshot_id],
                    Tags=tags
                )
                
            except Exception as tag_error:
                logger.warning(f"Failed to tag snapshot {snapshot_id}: {str(tag_error)}")
            
            logger.info(f"Created snapshot {snapshot_id} for volume {volume_id}")
            
            return {
                'volume_id': volume_id,
                'snapshot_id': snapshot_id,
                'description': description,
                'start_time': response['StartTime'].isoformat(),
                'volume_size': volume_size,
                'volume_type': volume_type,
                'availability_zone': availability_zone,
                'instance_id': instance_id,
                'device_name': device_name,
                'state': response['State'],
                'action': 'created'
            }
            
    except Exception as e:
        logger.error(f"Error creating snapshot for volume {volume_id}: {str(e)}")
        raise e

def cleanup_old_snapshots(ec2_client, retention_days, description_prefix, dry_run=False):
    """
    Delete snapshots older than the retention period.
    """
    cleanup_results = {
        'deleted_snapshots': [],
        'errors': []
    }
    
    try:
        # Calculate cutoff date
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        
        # Get all snapshots owned by this account
        response = ec2_client.describe_snapshots(OwnerIds=['self'])
        
        for snapshot in response['Snapshots']:
            snapshot_id = snapshot['SnapshotId']
            start_time = snapshot['StartTime'].replace(tzinfo=timezone.utc)
            description = snapshot.get('Description', '')
            volume_id = snapshot.get('VolumeId', 'unknown')
            volume_size = snapshot.get('VolumeSize', 0)
            
            # Check if this is an auto-created snapshot and if it's old enough
            if (description.startswith(description_prefix) and 
                start_time < cutoff_date):
                
                age_days = (datetime.now(timezone.utc) - start_time).days
                
                try:
                    if dry_run:
                        logger.info(f"DRY RUN: Would delete snapshot {snapshot_id} (age: {age_days} days)")
                        cleanup_results['deleted_snapshots'].append({
                            'snapshot_id': snapshot_id,
                            'volume_id': volume_id,
                            'description': description,
                            'start_time': start_time.isoformat(),
                            'age_days': age_days,
                            'volume_size': volume_size,
                            'action': 'would_delete'
                        })
                    else:
                        # Delete the snapshot
                        ec2_client.delete_snapshot(SnapshotId=snapshot_id)
                        
                        logger.info(f"Deleted snapshot {snapshot_id} (age: {age_days} days)")
                        
                        cleanup_results['deleted_snapshots'].append({
                            'snapshot_id': snapshot_id,
                            'volume_id': volume_id,
                            'description': description,
                            'start_time': start_time.isoformat(),
                            'age_days': age_days,
                            'volume_size': volume_size,
                            'action': 'deleted'
                        })
                        
                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    if error_code == 'InvalidSnapshot.InUse':
                        error_msg = f"Cannot delete snapshot {snapshot_id}: currently in use"
                    else:
                        error_msg = f"Error deleting snapshot {snapshot_id}: {str(e)}"
                    
                    logger.error(error_msg)
                    cleanup_results['errors'].append(error_msg)
                
                except Exception as e:
                    error_msg = f"Unexpected error deleting snapshot {snapshot_id}: {str(e)}"
                    logger.error(error_msg)
                    cleanup_results['errors'].append(error_msg)
                    
    except Exception as e:
        error_msg = f"Error during snapshot cleanup: {str(e)}"
        logger.error(error_msg)
        cleanup_results['errors'].append(error_msg)
    
    return cleanup_results

def generate_summary_report(results):
    """Generate a human-readable summary of the snapshot management results."""
    summary = f"""
EBS Snapshot Management Summary:
==============================
Volumes Processed: {results['volumes_processed']}
Snapshots Created: {len(results['snapshots_created'])}
Snapshots Deleted: {results['total_snapshots_cleaned']}
Retention Period: {results['retention_days']} days
Dry Run Mode: {results['dry_run']}
Errors: {len(results['errors'])}

Created Snapshots:
"""
    
    if results['snapshots_created']:
        for snapshot in results['snapshots_created']:
            summary += f"- {snapshot['snapshot_id']} for volume {snapshot['volume_id']} ({snapshot['volume_size']}GB)\n"
    else:
        summary += "None\n"
    
    if results['snapshots_deleted']:
        summary += "\nDeleted Snapshots:\n"
        for snapshot in results['snapshots_deleted']:
            summary += f"- {snapshot['snapshot_id']} (age: {snapshot['age_days']} days)\n"
    
    if results['errors']:
        summary += "\nErrors:\n"
        for error in results['errors']:
            summary += f"- {error}\n"
    
    return summary.strip()

def send_sns_notification(sns_client, topic_arn, results, summary):
    """Send SNS notification with snapshot management results."""
    try:
        message = {
            'operation': 'EBS_SNAPSHOT_MANAGEMENT',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'summary': summary,
            'snapshots_created': len(results['snapshots_created']),
            'snapshots_deleted': results['total_snapshots_cleaned'],
            'errors': len(results['errors']),
            'details': results
        }
        
        subject = f"EBS Snapshot Report: {len(results['snapshots_created'])} Created, {results['total_snapshots_cleaned']} Deleted"
        
        sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message, indent=2, default=str),
            Subject=subject
        )
        
        logger.info(f"SNS notification sent to {topic_arn}")
        
    except Exception as e:
        logger.error(f"Failed to send SNS notification: {str(e)}")