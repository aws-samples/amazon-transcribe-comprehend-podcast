import boto3
import random
import string
import uuid
import httplib
import urlparse
import json
import base64
import hashlib

"""
If included in a Cloudformation build as a CustomResource, generate a random string of length
given by the 'length' parameter.
By default the character set used is upper and lowercase ascii letters plus digits.
If the 'punctuation' parameter is specified this also includes punctuation.
If you specify a KMS key ID then it will be encrypted, too
"""

s3_client = boto3.client('s3')

def send_response(request, response, status=None, reason=None):
    if status is not None:
        response['Status'] = status

    if reason is not None:
        response['Reason'] = reason

    if 'ResponseURL' in request and request['ResponseURL']:
        url = urlparse.urlparse(request['ResponseURL'])
        body = json.dumps(response)
        print ('body', body)
        https = httplib.HTTPSConnection(url.hostname)
        https.request('PUT', url.path+'?'+url.query, body)

    return response


def lambda_handler(event, context):

    response = {
        'StackId': event['StackId'],
        'RequestId': event['RequestId'],
        'LogicalResourceId': event['LogicalResourceId'],
        'Status': 'SUCCESS'
    }

    if event['ResponseURL'] == '':
        s3params = {"Bucket": 'gillemi-gillemi', "Key": 'result.json'}
        event["ResponseURL"] = s3_client.generate_presigned_url('put_object', s3params)
        print('The URL is', event["ResponseURL"])
    

    if 'PhysicalResourceId' in event:
        response['PhysicalResourceId'] = event['PhysicalResourceId']
    else:
        response['PhysicalResourceId'] = str(uuid.uuid4())

    if event['RequestType'] == 'Delete':
        return send_response(event, response)


    length = 8
    try:
        length = int(event['ResourceProperties']['Length'])
    except:
        pass

   
    random_string = event['ResourceProperties']['StackName'][:12] + '-' + hashlib.sha224(event['StackId']).hexdigest()[:length]
 
    response['Data']   = { 'RandomString': random_string }
    response['Reason'] = 'Successfully generated a random string'
    return send_response(event, response)

