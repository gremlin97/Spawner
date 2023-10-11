import boto3
import time
import os
from ec2_metadata import ec2_metadata

REGION_NAME = 'us-east-1'
REQUEST_SQS='https://sqs.us-east-1.amazonaws.com/627642662710/Request-Queue'
RESPONSE_SQS='https://sqs.us-east-1.amazonaws.com/627642662710/Response-Queue'
INPUT_S3_BUCKET='image-inflow-bucket'
OUTPUT_S3_BUCKET = 'image-outflow-bucket'
INPUT_FOLDER = '/home/ubuntu/app-tier/data/imagenet-100-updated/imagenet-100'
RESULTS_FOLDER = '/home/ubuntu/app-tier'

def sqs_client():
    sqs = boto3.client(
        'sqs',
        aws_access_key_id='AKIAZEITK743DTGIAQM2',
        aws_secret_access_key='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',
        region_name=REGION_NAME)
    return sqs

def s3_client():
    s3 = boto3.client('s3', 
        aws_access_key_id='AKIAZEITK743DTGIAQM2',
        aws_secret_access_key='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', 
        region_name=REGION_NAME)
    return s3

# This function downnloads the image from the input bucket corresponding to the input image name
def download_image_from_bucket(image_name):
    s3 = s3_client()
    downloaded_image_path = f'{INPUT_FOLDER}/{image_name}'
    s3.download_file(INPUT_S3_BUCKET, image_name, downloaded_image_path)
    return downloaded_image_path

# This function classifies the given image
def classify_image(downloaded_image_path):
    stdout = os.popen(f'cd {RESULTS_FOLDER}; python3 image_classification.py "{downloaded_image_path}"')
    classified_result  = stdout.read().strip()
    return classified_result 

# This function sends the result to response queue
def send_result_to_response_queue(result_key, classified_result):
    sqs = sqs_client()
    instance_id = ec2_metadata.instance_id

    result_pair = f'({result_key},{classified_result},{instance_id})'
    sqs.send_message(
        QueueUrl=RESPONSE_SQS,
        MessageBody=result_pair,
        DelaySeconds=0
    )
# This function uploads the result to the output S3 bucket
def upload_result_to_output_bucket(result_key, classified_result):
    s3 = boto3.client('s3', aws_access_key_id='AKIAZEITK743DTGIAQM2',
        aws_secret_access_key='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', region_name='us-east-1')
    ec2 = boto3.client('ec2', aws_access_key_id='AKIAZEITK743DTGIAQM2', aws_secret_access_key='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', region_name = 'us-east-1',)
    instance_id = ec2.describe_instances()['Reservations'][0]['Instances'][0]['InstanceId']
    result_pair = f'({result_key}, {classified_result}, {instance_id})'
    print(result_pair)
    s3.put_object(Key=result_key, Bucket=OUTPUT_S3_BUCKET, Body=result_pair)
    

while True:
# receive a message from request queue
    sqs = sqs_client()
    received_message = sqs.receive_message(QueueUrl=REQUEST_SQS, MaxNumberOfMessages=1)
    images = received_message.get('Messages', [])

    for image in images:
        image_name = image['Body']
        receipt_handle = image['ReceiptHandle']  # Needed for message deletion

        #download corresponding image from input S3 bucket
        downloaded_image_path = download_image_from_bucket(image_name)
        print("Here:",downloaded_image_path)

        #do classification using given model
        classified_result = classify_image(downloaded_image_path)

        #upload result to output s3 bucket
        result_key = image_name.split('.')[0]
        upload_result_to_output_bucket(result_key, classified_result)

        #send result to response queue 
        send_result_to_response_queue(result_key, classified_result)

        #delete image from local folder
        os.remove(downloaded_image_path)

        #delete message from request queue
        sqs.delete_message(QueueUrl=REQUEST_SQS, ReceiptHandle=receipt_handle)

    # Wait for 5 seconds before polling next message 
    # time.sleep(8) 
    time.sleep(8) 