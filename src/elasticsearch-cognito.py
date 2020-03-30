import boto3
import random
import string
import uuid
import httplib
import urlparse
import json
import base64
import hashlib
import os
import cfnresponse
import logging

from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

es_client = boto3.client('es')
cognito_idp_client = boto3.client('cognito-idp')
# step_function_client = boto3.client('stepfunctions')


# Generates a random ID for the step function execution
def id_generator(size=12, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def pwd_generator(size=8):
    lowerChars = string.ascii_lowercase
    upperChars = string.ascii_uppercase
    digits = string.digits
    specials = '%$#&[]'
    return random.choice(lowerChars) + random.choice(upperChars) + random.choice(digits) + random.choice(
        specials) + random.choice(lowerChars) + random.choice(upperChars) + random.choice(digits) + random.choice(
        specials)


def configure_cognito_lambda_handler(event, context):
    logger.info("Received event: %s" % json.dumps(event))

    try:
        if event['RequestType'] == 'Create':
            create_response = create(event)
            cfnresponse.send(event, context, cfnresponse.SUCCESS, create_response)
        if event['RequestType'] == 'Update':
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
        elif event['RequestType'] == 'Delete':
            result_status = delete(event)
            cfnresponse.send(event, context, result_status, {})
    except:
        logger.error("Error", exc_info=True)
        cfnresponse.send(event, context, cfnresponse.FAILED, {})


def create(event):
    es_domain_name = event['ResourceProperties']['EsCluster']
    user_pool_id = event['ResourceProperties']['UserPoolId']

    create_user_pool_domain(es_domain_name, user_pool_id)

    kibana_user, kibana_password, kibana_email = get_user_credentials(event)
    add_user(user_pool_id, kibana_user, kibana_email, kibana_password)
    return {
        "KibanaUser": kibana_user,
        "KibanaPassword": kibana_password}



def delete(event):
    user_pool_id = event['ResourceProperties']['UserPoolId']
    es_domain_name = event['ResourceProperties']['EsCluster']

    delete_user_pool_domain(es_domain_name, user_pool_id)
    return cfnresponse.SUCCESS


def create_user_pool_domain(domain_name, user_pool_id):
    try:
        cognito_response = cognito_idp_client.create_user_pool_domain(
            Domain=domain_name,
            UserPoolId=user_pool_id
        )
        logger.info("create Cognito domain {} for user pool {} successful.".format(domain_name, user_pool_id))
        logger.debug(cognito_response)
    except ClientError as e:
        if 'InvalidParameterException' in e.response['Error']['Code'] and "exists" in e.response['Error']['Message']:
            logger.info("Domain [{}] already created for user pool [{}]".format(domain_name, user_pool_id))
            pass
        else:
            logger.error("got error creating domain in user pool [{}] : {}".format(user_pool_id, e.response['Error']))
            raise


def delete_user_pool_domain(domain_name, user_pool_id):
    try:
        cognito_response = cognito_idp_client.delete_user_pool_domain(
            Domain=domain_name,
            UserPoolId=user_pool_id
        )
        logger.info("deleted user pool: {}".format(user_pool_id))
        logger.debug(cognito_response)
    except ClientError as e:
        logger.error("Error deleting user pool", exc_info=True)
        raise


def get_user_credentials(event):
    if 'kibanaUser' in event['ResourceProperties'] and event['ResourceProperties']['kibanaUser'] != '':
        kibanaUser = event['ResourceProperties']['kibanaUser']
    else:
        kibanaUser = 'kibana'

    if 'kibanaEmail' in event['ResourceProperties'] and event['ResourceProperties']['kibanaEmail'] != '':
        kibanaEmail = event['ResourceProperties']['kibanaEmail']
    else:
        kibanaEmail = id_generator(6) + '@example.com'

    kibanaPassword = pwd_generator()
    return kibanaUser, kibanaPassword, kibanaEmail


# def lambda_handler(event, context):
#     try:
#         return process_cfn(event, context)
#     except Exception as e:
#         print("EXCEPTION", e)
#         print(e)
#         send_response(event, {
#             'StackId': event['StackId'],
#             'RequestId': event['RequestId'],
#             'LogicalResourceId': event['LogicalResourceId']
#         }, "FAILED")
#
#
# def process_cfn(event, context):
#     print("Received event: " + json.dumps(event, indent=2))
#
#     # stepFunctionArn = os.environ['STEP_FUNCTION_ARN']
#     #
#     # esDomainName = event['ResourceProperties']['esCluster']
#     # userPoolId = event['ResourceProperties']['UserPoolId']
#     # identityPoolId = event['ResourceProperties']['IdentityPoolId']
#     # esRoleArn = event['ResourceProperties']['esRoleArn']
#
#     if 'kibanaUser' in event['ResourceProperties'] and event['ResourceProperties']['kibanaUser'] != '':
#         kibanaUser = event['ResourceProperties']['kibanaUser']
#     else:
#         kibanaUser = 'kibana'
#
#     if 'kibanaEmail' in event['ResourceProperties'] and event['ResourceProperties']['kibanaEmail'] != '':
#         kibanaEmail = event['ResourceProperties']['kibanaEmail']
#     else:
#         kibanaEmail = id_generator(6) + '@example.com'
#
#     kibanaPassword = pwd_generator()
#
#     session = boto3.session.Session()
#
#     response = {
#         'StackId': event['StackId'],
#         'RequestId': event['RequestId'],
#         'LogicalResourceId': event['LogicalResourceId'],
#         'Status': 'IN_PROCESS',
#         'kibanaPassword': kibanaPassword,
#         'kibanaUser': kibanaUser
#     }
#
#     if 'PhysicalResourceId' in event:
#         response['PhysicalResourceId'] = event['PhysicalResourceId']
#     else:
#         response['PhysicalResourceId'] = esDomainName + '-cognito'
#
#     if event['RequestType'] == 'Delete':
#         try:
#             cognito_response = cognito_idp_client.delete_user_pool_domain(
#                 Domain=esDomainName,
#                 UserPoolId=userPoolId
#             )
#         except cognito_idp_client.exceptions.InvalidParameterException:
#             pass
#
#         send_response(event, response, status="SUCCESS", reason="User Pool Domain Deleted")
#         return
#
#     adduser_response = add_user(userPoolId, kibanaUser, kibanaEmail, kibanaPassword)
#
#     try:
#         cognito_response = cognito_idp_client.create_user_pool_domain(
#             Domain=esDomainName,
#             UserPoolId=userPoolId
#         )
#     except cognito_idp_client.exceptions.InvalidParameterException:
#         pass
#
#     cognitoOptions = {
#         "Enabled": True,
#         "UserPoolId": userPoolId,
#         "IdentityPoolId": identityPoolId,
#         "RoleArn": esRoleArn
#     }
#
#     es_response = es_client.update_elasticsearch_domain_config(
#         DomainName=esDomainName,
#         CognitoOptions=cognitoOptions)
#
#     response["DomainName"] = esDomainName
#
#     stepFunctionPayload = {"event": event, "response": response}
#     step_function_response = step_function_client.start_execution(
#         stateMachineArn=stepFunctionArn,
#         name=id_generator(),
#         input=json.dumps(stepFunctionPayload, indent=4, sort_keys=True, default=str)
#     )
#     return stepFunctionPayload


def add_user(userPoolId, kibanaUser, kibanaEmail, kibanaPassword):
    cognito_response = cognito_idp_client.admin_create_user(
        UserPoolId=userPoolId,
        Username=kibanaUser,
        UserAttributes=[
            {
                'Name': 'email',
                'Value': kibanaEmail
            },
            {
                'Name': 'email_verified',
                'Value': 'True'
            }
        ],
        TemporaryPassword=kibanaPassword,
        MessageAction='SUPPRESS',
        DesiredDeliveryMediums=[
            'EMAIL'
        ]
    )
    logger.info("create Cognito user {} for user pool {} successful.".format(kibanaUser, userPoolId))
    return cognito_response

# """
# If included in a Cloudformation build as a CustomResource, generate a random string of length
# given by the 'length' parameter.
# By default the character set used is upper and lowercase ascii letters plus digits.
# If the 'punctuation' parameter is specified this also includes punctuation.
# If you specify a KMS key ID then it will be encrypted, too
# """
#
# s3_client = boto3.client('s3')
#
#
# def send_response(request, response, status=None, reason=None):
#     if status is not None:
#         response['Status'] = status
#
#     if reason is not None:
#         response['Reason'] = reason
#
#     if not 'PhysicalResourceId' in response or response['PhysicalResourceId']:
#         response['PhysicalResourceId'] = ''.join(
#             random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
#
#     if request['ResponseURL'] == '':
#         s3params = {"Bucket": 'gillemi-gillemi', "Key": 'result.json'}
#         request['ResponseURL'] = s3_client.generate_presigned_url('put_object', s3params)
#         print('The debug URL is', request['ResponseURL'])
#
#     if 'ResponseURL' in request and request['ResponseURL']:
#         url = urlparse.urlparse(request['ResponseURL'])
#         body = json.dumps(response)
#         print('body', url, body)
#         https = httplib.HTTPSConnection(url.hostname)
#         https.request('PUT', url.path + '?' + url.query, body)
#
#     return response

# def check_status(event, context):
#     print("Received event: " + json.dumps(event, indent=2))
#
#     if "IsFail" in event["event"]:
#         send_response(event["event"], {}, status="FAILED", reason="Forced Error")
#         return
#
#
#     if event["event"]["RequestType"] == "Delete":
#         try:
#             es_response = es_client.describe_elasticsearch_domain(DomainName=event["response"]["PhysicalResourceId"])
#
#
#         except es_client.exceptions.ResourceNotFoundException:
#             print('Domain not found - delete Successful')
#
#             event["response"]["Status"] = "SUCCESS"
#             event["response"]['Reason'] = 'Successful'
#
#             if event["event"]["ResponseURL"] == '':
#                 s3params = {"Bucket": 'gillemi-gillemi', "Key": 'result.json'}
#                 event["event"]["ResponseURL"] = s3_client.generate_presigned_url('put_object', s3params)
#                 print('The URL is', event["event"]["ResponseURL"])
#
#             send_response(event["event"], event["response"])
#
#         return event
#
#     es_response = es_client.describe_elasticsearch_domain(DomainName=event["response"]["DomainName"])
#
#     if es_response["DomainStatus"]["Processing"] == False and 'Endpoint' in es_response["DomainStatus"]:
#         event["response"]["Status"] = "SUCCESS"
#         event["response"]["Data"]   = {
#             "DomainName": event["response"]["DomainName"],
#             "Endpoint":  es_response["DomainStatus"]["Endpoint"],
#             "KibanaUser": event["response"]["kibanaUser"],
#             "KibanaPassword": event["response"]["kibanaPassword"]
#         }
#         event["response"]['Reason'] = 'Successful'
#
#         send_response(event["event"], event["response"])
#
#     return event
