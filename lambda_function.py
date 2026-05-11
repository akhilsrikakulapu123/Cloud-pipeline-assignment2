import json
import boto3
import os
import datetime
import logging
import csv
from io import StringIO
from botocore.exceptions import ClientError

# AWS Clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

# Environment Variables
BUCKET_NAME = os.environ['BUCKET_NAME']
TABLE_NAME = os.environ['TABLE_NAME']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

# DynamoDB Table
table = dynamodb.Table(TABLE_NAME)

# Logger Configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def generate_txt_report(today):
    return f"""
Daily Summary Report
Date: {today}

Status: SUCCESS
Processed Successfully.
"""


def generate_json_report(today):
    return json.dumps({
        "date": today,
        "status": "SUCCESS",
        "message": "Processed Successfully"
    }, indent=4)


def generate_csv_report(today):
    output = StringIO()
    writer = csv.writer(output)

    writer.writerow(["Date", "Status", "Message"])
    writer.writerow([today, "SUCCESS", "Processed Successfully"])

    return output.getvalue()


def upload_to_s3(file_name, content):
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=content
        )

        logger.info(f"Uploaded {file_name} to S3")

    except ClientError as e:
        logger.error(f"S3 Upload Failed: {str(e)}")
        raise e


def store_metadata(today, files_uploaded):
    try:
        table.put_item(
            Item={
                'report_date': today,
                'files_uploaded': files_uploaded,
                'timestamp': str(datetime.datetime.utcnow()),
                'status': 'SUCCESS'
            }
        )

        logger.info("Metadata stored in DynamoDB")

    except ClientError as e:
        logger.error(f"DynamoDB Error: {str(e)}")
        raise e


def send_notification(today):
    try:
        message = f"""
Report Generation Successful

Date: {today}

Reports uploaded to S3 successfully.
"""

        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject='AWS Report Pipeline Success',
            Message=message
        )

        logger.info("SNS Notification Sent")

    except ClientError as e:
        logger.error(f"SNS Error: {str(e)}")
        raise e


def lambda_handler(event, context):

    start_time = datetime.datetime.utcnow()

    today = datetime.datetime.now().strftime('%Y-%m-%d')

    txt_report = generate_txt_report(today)
    json_report = generate_json_report(today)
    csv_report = generate_csv_report(today)

    files = [
        (f'reports/report-{today}.txt', txt_report),
        (f'reports/report-{today}.json', json_report),
        (f'reports/report-{today}.csv', csv_report)
    ]

    uploaded_files = []

    try:

        # Upload Reports
        for file_name, content in files:
            upload_to_s3(file_name, content)
            uploaded_files.append(file_name)

        # Store Metadata
        store_metadata(today, uploaded_files)

        # Send Notification
        send_notification(today)

        execution_time = (
            datetime.datetime.utcnow() - start_time
        ).total_seconds()

        logger.info(f"Execution Time: {execution_time} seconds")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Reports generated successfully',
                'files_uploaded': uploaded_files,
                'execution_time_seconds': execution_time
            })
        }

    except Exception as e:

        logger.error(f"Pipeline Failed: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
