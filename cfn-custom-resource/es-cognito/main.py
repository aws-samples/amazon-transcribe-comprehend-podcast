import boto3
import random
import string
import json
import cfnresponse
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cognito_idp_client = boto3.client('cognito-idp')


# Generates a random ID
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
    user_pool_id = event['ResourceProperties']['UserPoolId']

    kibana_user, kibana_password, kibana_email = get_user_credentials(event)
    add_user(user_pool_id, kibana_user, kibana_email, kibana_password)
    return {
        "KibanaUser": kibana_user,
        "KibanaPassword": kibana_password}


def delete(event):
    return cfnresponse.SUCCESS


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
