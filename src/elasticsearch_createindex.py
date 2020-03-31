from __future__ import print_function

import boto3
import certifi
import json
import os
from aws_requests_auth.aws_auth import AWSRequestsAuth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import logging
import time

# Log level
logging.basicConfig()
logger = logging.getLogger()
if os.getenv('LOG_LEVEL') == 'DEBUG':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

# Parameters
REGION = os.getenv('AWS_REGION', default='us-east-1')

# Pull environment data for the ES domain
esendpoint = os.environ['ES_DOMAIN']
# If debug mode is TRUE, then S3 files are not deleted
isDebugMode = os.environ['DEBUG_MODE']

# get the Elasticsearch index name from the environment variables
FULL_EPISODE_INDEX = os.getenv('ES_EPISODE_INDEX', default='episodes')
# get the Elasticsearch index name from the environment variables
KEYWORDS_INDEX = os.getenv('ES_PARAGRAPH_INDEX', default='paragraphs')

s3_client = boto3.client('s3')
# Create the auth token for the sigv4 signature
session = boto3.session.Session()
credentials = session.get_credentials().get_frozen_credentials()
awsauth = AWSRequestsAuth(
    aws_access_key=credentials.access_key,
    aws_secret_access_key=credentials.secret_key,
    aws_token=credentials.token,
    aws_host=esendpoint,
    aws_region=REGION,
    aws_service='es'
)

# Connect to the elasticsearch cluster using aws authentication. The lambda function
# must have access in an IAM policy to the ES cluster.
es = Elasticsearch(
    hosts=[{'host': esendpoint, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    ca_certs=certifi.where(),
    timeout=120,
    connection_class=RequestsHttpConnection
)


def create_episode_index():
    mappings = '''
    {
        "mappings": {
            "properties": {
                "audio_url":{
                    "type": "keyword"
                },
                "audio_type":{
                    "type": "keyword"
                },
                "transcript":{
                    "type": "text"
                },
                "audio_s3_location": {
                    "type": "keyword"
                },
                "published_time":{
                    "type":   "date",
                    "format": "yyyy:MM:dd HH:mm:ss"
                },
                "summary": {
                    "type": "text"
                },
                "source_feed":{
                    "type": "keyword"
                }
            }
        }
    }
    '''
    start = time.time()
    logger.info("mappings to create for index: " + mappings)
    res = es.indices.create(index=FULL_EPISODE_INDEX, body=mappings)
    round_trip = time.time() - start
    logger.info(json.dumps(res, indent=2))
    logger.info('REQUEST_TIME es_client.indices.create {:10.4f}'.format(round_trip))


def lambda_handler(event, context):
    if not es.indices.exists(index=FULL_EPISODE_INDEX):
        create_episode_index()
    else:
        logger.info("index " + FULL_EPISODE_INDEX + " already exists. skipping index creation.")
