from __future__ import print_function
import boto3
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import os
import logging
from common_lib import id_generator

# Log level
logging.basicConfig()
logger = logging.getLogger()
if os.getenv('LOG_LEVEL') == 'DEBUG':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')


# This is the entry point for the lambda function.
# {
#  "Episode": "Name of the podcast from the RSS feed",
#  "PodcastName": "Name of the podcast",
#  "bucket": "The bucket where the data will be stored",
#  "dryrun": "Tells the step function to skip this step. Won't impact this function",
#  "podcastUrl": "The url of the mp3 file provided by the RSS feed."
# }
def lambda_handler(event, context):
    url = event['podcastUrl']
    bucket = event['bucket']
    content_type = event['audio_type']

    # generate a temp file name to store in S3
    key = 'podcasts/audio/' + id_generator() + "-" + os.path.basename(url)

    try:
        logger.info("downloading from: " + url)

        # Open the url
        stream = urlopen(url)

        s3_object_metadata = {'href': url}

        logger.info("writing to s3://" + bucket + "/" + key)
        s3_client.upload_fileobj(
            Fileobj=stream,
            Bucket=bucket,
            Key=key,
            ExtraArgs={
                "Metadata": s3_object_metadata,
                'ContentType': content_type
            }
        )
        logger.info("done writing to s3://" + bucket + "/" + key)

        # Return the bucket and key the location of the podcast file stored in S3
        return {
            "bucket": bucket,
            "key": key
        }

    # handle errors
    except HTTPError as e:
        logger.error("HTTPError downloading:" + url)
        logger.exception(str(e))
        raise e
    except URLError as e:
        logger.error("URLError downloading:" + url)
        logger.exception(str(e))
        raise e
    except Exception as e:
        logger.error("Unexpected error:")
        logger.exception(str(e))
        raise e
