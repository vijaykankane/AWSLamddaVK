import boto3
import json
import logging
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda function to automatically manage EC2 instances based on tags.
    Stops instances tagged with Action=Auto-Stop
    Starts instances tagged with Action=Auto-Start
    """
    
    # Initialize EC2 client
    try:
        ec2_client = boto3.client('ec2')
        logger.info("Successfully initialized EC2 client")
    except Exception as e:
        logger.error(f"Failed to initialize EC2 client: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error initializing EC2 client: {str(e)}')
        }
    
    results = {
        'stopped_instances': [],
        'started_instances': [],
        'errors': []
    }
    
    try:
        # Get all EC2 instances with Action tags
        response = ec2_client.describe_instances(
            Filters=[
                {
                    'Name': 'tag:Action',
                    'Values': ['Auto-Stop', 'Auto-Start']
                },
                {
                    'Name': 'instance-state-name',
                    'Values': ['running', 'stopped', 'stopping', 'pending']
                }
            ]
        )
        
        instances_to_stop = []
        instances_to_start = []
        
        # Process instances based on their tags
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                instance_id = instance['InstanceId']
                instance_state = instance['State']['Name']
                
                # Find the Action tag
                action_tag = None
                for tag in instance.get('Tags', []):
                    if tag['Key'] == 'Action':
                        action_tag = tag['Value']
                        break
                
                if action_tag == 'Auto-Stop' and instance_state == 'running':
                    instances_to_stop.append(instance_id)
                elif action_tag == 'Auto-Start' and instance_state == 'stopped':
                    instances_to_start.append(instance_id)
                
                logger.info(f"Instance {instance_id}: State={instance_state}, Action={action_tag}")
        
        # Stop instances tagged with Auto-Stop
        if instances_to_stop:
            try:
                stop_response = ec2_client.stop_instances(InstanceIds=instances_to_stop)
                for instance in stop_response['StoppingInstances']:
                    instance_id = instance['InstanceId']
                    current_state = instance['CurrentState']['Name']
                    results['stopped_instances'].append({
                        'instance_id': instance_id,
                        'current_state': current_state
                    })
                    logger.info(f"Successfully initiated stop for instance {instance_id}")
            except ClientError as e:
                error_msg = f"Failed to stop instances {instances_to_stop}: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
        
        # Start instances tagged with Auto-Start
        if instances_to_start:
            try:
                start_response = ec2_client.start_instances(InstanceIds=instances_to_start)
                for instance in start_response['StartingInstances']:
                    instance_id = instance['InstanceId']
                    current_state = instance['CurrentState']['Name']
                    results['started_instances'].append({
                        'instance_id': instance_id,
                        'current_state': current_state
                    })
                    logger.info(f"Successfully initiated start for instance {instance_id}")
            except ClientError as e:
                error_msg = f"Failed to start instances {instances_to_start}: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
        
        # Log summary
        logger.info(f"Operation completed. Stopped: {len(results['stopped_instances'])}, "
                   f"Started: {len(results['started_instances'])}, "
                   f"Errors: {len(results['errors'])}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'EC2 instance management completed successfully',
                'results': results
            }, indent=2)
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