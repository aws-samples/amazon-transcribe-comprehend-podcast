from __future__ import print_function
import boto3
import json
import datetime
from time import mktime
import os
from common_lib import id_generator
import logging
from botocore.config import Config

# Log level
logging.basicConfig()
logger = logging.getLogger()
if os.getenv('LOG_LEVEL') == 'DEBUG':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


class ThrottlingException(Exception):
    pass


CONTENT_TYPE_TO_MEDIA_FORMAT = {
    "audio/mpeg": "mp3",
    "audio/wav": "wav",
    "audio/flac": "flac",
    "audio/mp4a-latm": "mp4"}


class InvalidInputError(ValueError):
    pass


# Custom encoder for datetime objects
class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return int(mktime(obj.timetuple()))
        return json.JSONEncoder.default(self, obj)


# limit the number of retries submitted by boto3 because Step Functions will handle the exponential retries more efficiently
config = Config(
    retries=dict(
        max_attempts=2
    )
)

client = boto3.client('transcribe', config=config)


# Entrypoint for lambda funciton
def lambda_handler(event, context):
    session = boto3.session.Session()
    region = session.region_name

    # Default to unsuccessful
    isSuccessful = "FALSE"

    # Create a random name for the transcription job
    jobname = id_generator()

    # Extract the bucket and key from the downloadPodcast lambda function
    bucket = event['audioS3Location']['bucket']
    key = event['audioS3Location']['key']

    content_type = event['audio_type']
    if content_type not in CONTENT_TYPE_TO_MEDIA_FORMAT:
        raise InvalidInputError(content_type + " is not supported audio type.")
    media_type = CONTENT_TYPE_TO_MEDIA_FORMAT[content_type]
    logger.info("media type: " + content_type)

    # Assemble the url for the object for transcribe. It must be an s3 url in the region
    url = "https://s3-" + region + ".amazonaws.com/" + bucket + "/" + key

    try:
        settings = {
            'VocabularyName': event['vocabularyInfo']['name'],
            'ShowSpeakerLabels': False
        }

        if int(event['speakers']) > 1:
            settings['ShowSpeakerLabels'] = True
            settings['MaxSpeakerLabels'] = max(int(event['speakers']), 4)

        # Call the AWS SDK to initiate the transcription job.
        response = client.start_transcription_job(
            TranscriptionJobName=jobname,
            LanguageCode='en-US',
            Settings=settings,
            MediaFormat=media_type,
            Media={
                'MediaFileUri': url
            }
        )
        isSuccessful = "TRUE"
    except client.exceptions.BadRequestException as e:
        # There is a limit to how many transcribe jobs can run concurrently. If you hit this limit,
        # return unsuccessful and the step function will retry.
        logger.error(str(e))
        raise ThrottlingException(e)
    except client.exceptions.LimitExceededException as e:
        # There is a limit to how many transcribe jobs can run concurrently. If you hit this limit,
        # return unsuccessful and the step function will retry.
        logger.error(str(e))
        raise ThrottlingException(e)
    except client.exceptions.ClientError as e:
        # Return the transcription job and the success code
        # There is a limit to how many transcribe jobs can run concurrently. If you hit this limit,
        # return unsuccessful and the step function will retry.
        logger.error(str(e))
        raise ThrottlingException(e)
    return {
        "success": isSuccessful,
        "transcribeJob": jobname
    }
