from __future__ import print_function
import boto3
from botocore.client import Config
import datetime
    

# The entry point for the lambda function
def lambda_handler(event, context):
    transcribeJob = event['transcribeJob']
    client = boto3.client('transcribe')
    
    # Call the AWS SDK to get the status of the transcription job
    response = client.get_transcription_job(TranscriptionJobName=transcribeJob)
    
    # Pull the status
    status = response['TranscriptionJob']['TranscriptionJobStatus']
    
    retval = {
        "status": status
    }
    
    # If the status is completed, return the transcription file url. This will be a signed url
    # that will provide the full details on the transcription
    if status == 'COMPLETED':
        retval["transcriptionUrl"] = response['TranscriptionJob']['Transcript']['TranscriptFileUri']
    
    return retval