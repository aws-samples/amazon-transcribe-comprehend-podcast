from __future__ import print_function

import boto3
import certifi
import json
import os
from aws_requests_auth.aws_auth import AWSRequestsAuth
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers
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


# Entry point into the lambda function
def lambda_handler(event, context):
    # Pull the keywords S3 location for the payload of the previous lambda function
    keywordsS3Location = event["processedTranscription"][0]

    fullEpisodeS3Location = event["processedTranscription"][1]

    index_keywords(es, event, keywordsS3Location)

    index_episode(es, event, fullEpisodeS3Location)
    # Episode level payload

    # If it is not debug mode, then clean up the temp files.
    if isDebugMode != 'TRUE':
        response = s3_client.delete_object(Bucket=event['audioS3Location']['bucket'],
                                           Key=event['audioS3Location']['key'])
        response = s3_client.delete_object(Bucket=keywordsS3Location['bucket'], Key=keywordsS3Location['key'])

    return


def index_episode(es, event, fullEpisodeS3Location):
    response = s3_client.get_object(Bucket=fullEpisodeS3Location['bucket'], Key=fullEpisodeS3Location['key'])
    file_content = response['Body'].read().decode('utf-8')
    fullepisode = json.loads(file_content)
    audio_url = event['podcastUrl']

    s3_location = "s3://" + event['audioS3Location']['bucket'] + "/" + event['audioS3Location']['key']

    doc = {
        'audio_url': audio_url,
        'audio_type': event['audio_type'],
        'title': event['Episode'],
        'summary': event['summary'],
        'published_time': event['publishTime'],
        'source_feed': event['sourceFeed'],
        'audio_s3_location': s3_location,
        'transcript': fullepisode['transcript'],
        'transcript_entities': fullepisode['transcript_entities']
    }

    if 'speakerNames' in event and len(event['speakerNames']) > 1:
        doc['speakerNames'] = event['speakerNames']

    logger.info("request")
    logger.debug(json.dumps(doc))
    # add the document to the index
    start = time.time()
    res = es.index(index=FULL_EPISODE_INDEX,
                   body=doc, id=audio_url)
    logger.info("response")
    logger.info(json.dumps(res, indent=4))
    logger.info('REQUEST_TIME es_client.index {:10.4f}'.format(time.time() - start))


def index_keywords(es, event, keywordsS3Location):
    # This is the number of seconds before the start time of the word to place
    # the hyperlink. This gives the listener some context before the word is spoken
    # to the discussion. Also browsers are precise when seeking and there is some
    # variation across browsers to the accuracy of the seek function. 10 seconds
    # is usually good, but occasionally you'll land after the word was spoken.
    audioOffset = int(os.environ['AUDIO_OFFSET'])

    response = s3_client.get_object(Bucket=keywordsS3Location['bucket'], Key=keywordsS3Location['key'])
    file_content = response['Body'].read().decode('utf-8')
    keywords = json.loads(file_content)
    actions = []
    # Iterate through all the keywords and create an index document for each phrase
    for i in range(len(keywords)):
        keyword = keywords[i]["text"]
        tags = keywords[i]["tags"]
        # Offset the time that the word was spoken to the listener has some context to the phrase
        time = str(max(float(keywords[i]["startTime"]) - audioOffset, 0))
        actions.append({
            "_index": KEYWORDS_INDEX,
            "_type": "_doc",
            "_source": {
                "PodcastName": event["PodcastName"],
                "Episode": event["Episode"],
                "url": event["podcastUrl"] + "#t=" + time,
                "text": keyword,
                "tags": tags,
                "speaker": keywords[i]["speaker"],
                "startTime": float(time)
            }
        })

    # Bulk load the documents into the index.
    result = helpers.bulk(es, actions)

    logger.info("indexed keywords to ES")
    logger.info(json.dumps(result, indent=2))
    return result

