import json
import boto3

# Initialize Glue client
glue = boto3.client("glue")

# CHANGE THIS to your actual Glue job name
GLUE_JOB_NAME = "demo"

def lambda_handler(event, context):
    # Log full event for debugging
    print("Received Event:")
    print(json.dumps(event))

    # Extract bucket and object key from EventBridge event
    bucket = event["detail"]["bucket"]["name"]
    key = event["detail"]["object"]["key"]

    # Safety check: process only CSV files
    if not key.lower().endswith(".csv"):
        print(f"Skipping non-CSV file: {key}")
        return {
            "statusCode": 200,
            "message": "Not a CSV file, skipped"
        }

    print(f"Triggering Glue job for s3://{bucket}/{key}")

    # Start Glue job
    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--SOURCE_BUCKET": bucket,
            "--SOURCE_KEY": key
        }
    )

    print("Glue JobRunId:", response["JobRunId"])

    return {
        "statusCode": 200,
        "message": "Glue job triggered successfully",
        "JobRunId": response["JobRunId"]
    }