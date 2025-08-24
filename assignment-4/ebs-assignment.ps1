# EBS Snapshot Assignment - FIXED VERSION
# Run this in PowerShell with AWS CLI configured

$ErrorActionPreference = "Continue"

# Get current AWS region and account
$region = aws configure get region
if (-not $region) {
    $region = "us-east-1"
}
$accountId = aws sts get-caller-identity --query 'Account' --output text
$availabilityZone = "${region}a"

Write-Host "=== EBS Snapshot Assignment Setup ===" -ForegroundColor Cyan
Write-Host "Region: $region" -ForegroundColor Green
Write-Host "Account: $accountId" -ForegroundColor Green

# Step 1: Create EBS Volume
Write-Host "`n1. Creating EBS volume..." -ForegroundColor Yellow
$volumeId = aws ec2 create-volume --size 1 --volume-type gp3 --availability-zone $availabilityZone --query 'VolumeId' --output text
Write-Host "Volume created: $volumeId"
aws ec2 wait volume-available --volume-ids $volumeId

# Step 2: Clean up any existing role and create new one
Write-Host "`n2. Setting up IAM role..." -ForegroundColor Yellow

# Delete existing role if it exists
$existingRole = aws iam get-role --role-name EBSSnapshotRole 2>$null
if ($existingRole) {
    Write-Host "Cleaning up existing role..."
    aws iam detach-role-policy --role-name EBSSnapshotRole --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole 2>$null
    aws iam detach-role-policy --role-name EBSSnapshotRole --policy-arn arn:aws:iam::aws:policy/AmazonEC2FullAccess 2>$null
    aws iam delete-role --role-name EBSSnapshotRole 2>$null
    Start-Sleep -Seconds 5
}



Write-Host "Creating role with inline JSON..."
$roleCreation = aws iam create-role --role-name EBSSnapshotRole --assume-role-policy-document '{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"lambda.amazonaws.com\"},\"Action\":\"sts:AssumeRole\"}]}'

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Role creation failed!" -ForegroundColor Red
    exit 1
}


# Attach policies
Write-Host "Attaching policies..."
aws iam attach-role-policy --role-name EBSSnapshotRole --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam attach-role-policy --role-name EBSSnapshotRole --policy-arn arn:aws:iam::aws:policy/AmazonEC2FullAccess

$roleArn = "arn:aws:iam::${accountId}:role/EBSSnapshotRole"
Write-Host "Role created: $roleArn" -ForegroundColor Green

# Step 3: Create Lambda function
Write-Host "`n3. Creating Lambda function..." -ForegroundColor Yellow

# Create Lambda code
$lambdaCode = @"
import boto3
import json
from datetime import datetime, timedelta

def lambda_handler(event, context):
    print('Lambda function started')
    ec2 = boto3.client('ec2')
    
    # Volume ID
    volume_id = '$volumeId'
    print(f'Processing volume: {volume_id}')
    
    try:
        # Create snapshot
        print('Creating snapshot...')
        response = ec2.create_snapshot(
            VolumeId=volume_id,
            Description=f'Automated snapshot of {volume_id} - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        )
        snapshot_id = response['SnapshotId']
        print(f'Created snapshot: {snapshot_id}')
        
        # Get all snapshots owned by this account
        print('Checking for old snapshots...')
        snapshots = ec2.describe_snapshots(OwnerIds=['self'])['Snapshots']
        
        # Calculate cutoff date (30 days ago)
        cutoff_date = datetime.now() - timedelta(days=30)
        
        deleted_snapshots = []
        
        # Delete old snapshots
        for snapshot in snapshots:
            snapshot_date = snapshot['StartTime'].replace(tzinfo=None)
            if snapshot_date < cutoff_date:
                try:
                    print(f'Deleting old snapshot: {snapshot["SnapshotId"]} from {snapshot_date}')
                    ec2.delete_snapshot(SnapshotId=snapshot['SnapshotId'])
                    deleted_snapshots.append(snapshot['SnapshotId'])
                except Exception as e:
                    print(f'Could not delete {snapshot["SnapshotId"]}: {str(e)}')
        
        result = {
            'statusCode': 200,
            'body': {
                'created_snapshot': snapshot_id,
                'deleted_snapshots': deleted_snapshots,
                'message': f'Successfully created 1 snapshot and deleted {len(deleted_snapshots)} old snapshots'
            }
        }
        print(f'Function completed successfully: {result}')
        return result
        
    except Exception as e:
        error_msg = f'Error in lambda_handler: {str(e)}'
        print(error_msg)
        return {
            'statusCode': 500,
            'body': {'error': error_msg}
        }
"@

$lambdaCode | Out-File -FilePath "lambda_function.py" -Encoding utf8

# Create ZIP file
if (Test-Path "lambda_function.zip") { Remove-Item "lambda_function.zip" }
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [System.IO.Compression.ZipFile]::Open("$PWD\lambda_function.zip", [System.IO.Compression.ZipArchiveMode]::Create)
$entry = $zip.CreateEntry("lambda_function.py")
$entryWriter = New-Object System.IO.StreamWriter($entry.Open())
$entryWriter.Write($lambdaCode)
$entryWriter.Close()
$zip.Dispose()

# Wait for IAM role propagation
Write-Host "Waiting 60 seconds for IAM role propagation..." -ForegroundColor Yellow
Start-Sleep -Seconds 60

# Delete existing function if it exists
aws lambda delete-function --function-name EBSSnapshotFunction 2>$null

# Create Lambda function
Write-Host "Creating Lambda function..."
$functionResult = aws lambda create-function `
    --function-name EBSSnapshotFunction `
    --runtime python3.9 `
    --role $roleArn `
    --handler lambda_function.lambda_handler `
    --zip-file fileb://lambda_function.zip `
    --timeout 300 `
    --description "Automated EBS snapshot and cleanup function"

if ($LASTEXITCODE -ne 0) {
    Write-Host "Lambda creation failed. Waiting additional 30 seconds..." -ForegroundColor Yellow
    Start-Sleep -Seconds 30
    $functionResult = aws lambda create-function `
        --function-name EBSSnapshotFunction `
        --runtime python3.9 `
        --role $roleArn `
        --handler lambda_function.lambda_handler `
        --zip-file fileb://lambda_function.zip `
        --timeout 300 `
        --description "Automated EBS snapshot and cleanup function"
        
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Lambda creation failed after retry" -ForegroundColor Red
        exit 1
    }
}

$functionArn = aws lambda get-function --function-name EBSSnapshotFunction --query 'Configuration.FunctionArn' --output text
Write-Host "Lambda function created: $functionArn" -ForegroundColor Green

# Step 4: Test the function
Write-Host "`n4. Testing Lambda function..." -ForegroundColor Yellow
$invokeResult = aws lambda invoke --function-name EBSSnapshotFunction --payload '{}' response.json
if (Test-Path "response.json") {
    $response = Get-Content "response.json" -Raw
    Write-Host "Lambda Response: $response" -ForegroundColor Green
    
    # Get logs
    Start-Sleep -Seconds 5
    $logGroup = "/aws/lambda/EBSSnapshotFunction"
    Write-Host "`nRecent logs:"
    aws logs describe-log-streams --log-group-name $logGroup --order-by LastEventTime --descending --max-items 1 --query 'logStreams[0].logStreamName' --output text | ForEach-Object {
        if ($_) {
            aws logs get-log-events --log-group-name $logGroup --log-stream-name $_ --query 'events[-10:].message' --output text
        }
    }
}

# Step 5: Create CloudWatch Events (optional)
Write-Host "`n5. Setting up weekly schedule..." -ForegroundColor Yellow
aws events put-rule --name EBSSnapshotSchedule --schedule-expression "rate(7 days)" --description "Weekly EBS snapshot cleanup"

# Add permission for CloudWatch Events
$sourceArn = "arn:aws:events:${region}:${accountId}:rule/EBSSnapshotSchedule"
aws lambda add-permission `
    --function-name EBSSnapshotFunction `
    --statement-id allow-cloudwatch-events `
    --action lambda:InvokeFunction `
    --principal events.amazonaws.com `
    --source-arn $sourceArn

# Add target
aws events put-targets --rule EBSSnapshotSchedule --targets "Id=1,Arn=$functionArn"

# Verify snapshots
Write-Host "`n6. Verifying snapshots..." -ForegroundColor Yellow
aws ec2 describe-snapshots --owner-ids self --query 'Snapshots[*].[SnapshotId,StartTime,Description]' --output table

Write-Host "`n=== ASSIGNMENT COMPLETED SUCCESSFULLY ===" -ForegroundColor Green
Write-Host "✅ EBS Volume: $volumeId" -ForegroundColor Cyan
Write-Host "✅ Lambda Function: $functionArn" -ForegroundColor Cyan
Write-Host "✅ IAM Role: $roleArn" -ForegroundColor Cyan
Write-Host "✅ CloudWatch Schedule: EBSSnapshotSchedule" -ForegroundColor Cyan

# Cleanup
Remove-Item "trust-policy.json", "lambda_function.py", "lambda_function.zip", "response.json" -ErrorAction SilentlyContinue

Write-Host "`nManual test command:" -ForegroundColor Yellow
Write-Host "aws lambda invoke --function-name EBSSnapshotFunction --payload '{}' test-response.json" -ForegroundColor White