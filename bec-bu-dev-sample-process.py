import boto3
import json
import time

""" Version 
Version Number : 1.0
Name           : bec-bu-dev-sample-process
Description    : This lambda python code developed for invoking kinesis stream to push data from S3 in development area
"""

# Creating Kinesis and S3 connection 

ks_client = boto3.client('kinesis', region_name='us-east-1')
s3_client = boto3.client('s3')

# S3 Bucket To Source Kinesis Stream
bucket_name = 'samplemessage'

# Kinesis Stream Name to target Kinesis Stream
stream_name = 'BUKinesisStream'

#Lambda Handler to invoke the process

def lambda_handler(event, context):
    # Looping all files available in S3 bucket and load as message data	
    for key in s3_client.list_objects(Bucket=bucket_name)['Contents']:
        filename = key['Key']
        json_obj = s3_client.get_object(Bucket=bucket_name, Key=filename)['Body'].read().decode('utf-8')
        message_data = json.loads(json_obj)
        partkey_data = str(stream_name+filename)
        # write the data to the stream by calling function 
        put_to_stream(message_data,partkey_data)
        # Providing sequance of message after interval of 1 second
        time.sleep(1)

# Function to take massage data from handler and put into stream
def put_to_stream(message_data,partkey_data):
        payload = message_data
        print (payload)
        put_response = ks_client.put_record(
                        StreamName=stream_name,
                        Data=json.dumps(payload),
                        PartitionKey=partkey_data)