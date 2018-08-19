from __future__ import print_function
import json
import os
import boto3
from urllib2 import urlopen, URLError, HTTPError
import xml.etree.ElementTree as ET
import logging
from dateutil import parser
from common_lib import find_duplicate_person, id_generator

client = boto3.client('comprehend')

# Log level
logging.basicConfig()
logger = logging.getLogger()
if os.getenv('LOG_LEVEL') == 'DEBUG':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


class InvalidInputError(ValueError):
    pass


# Entry point for the lambda function
def lambda_handler(event, context):
    logger.info("Received event: " + json.dumps(event, indent=2))
    feed_url = event['rss']
    max_episodes_to_process = None
    if 'maxEpisodesToProcess' in event:
        max_episodes_to_process = int(event['maxEpisodesToProcess'])

    maxConcurrentEpisodes = 10

    # Open the url and process the RSS feed
    retval = []
    bucket = os.environ['BUCKET_NAME']

    episode_count = 0

    # This array holds the entity types that are included in the custom vocabulary
    vocabularyTypes = ['COMMERCIAL_ITEM', 'EVENT', 'LOCATION', 'ORGANIZATION', 'TITLE']
    vocabularyItems = []

    try:
        filename = '/tmp/' + id_generator() + '.rss'
        # HTTP GET the RSS feed XML file
        f = urlopen(feed_url)

        # Open our local file for writing
        with open(filename, "wb") as local_file:
            local_file.write(f.read())

        # The RSS feed is an XML file, so parse it and traverse the tree and pull all the /channel/items
        tree = ET.parse(filename)
        root = tree.getroot()

        # Extract the title of the podcast
        channelTitle = root.find('channel/title')

        for child in root.findall('channel/item'):
            title = child.find('title')
            envelope = child.find('enclosure')

            date_entry = child.find('pubDate').text
            dt = parser.parse(date_entry)
            date_string = dt.strftime("%Y:%m:%d %H:%M:%S")

            keywords = []

            description = child.find('description').text
            description = description[0:4900]

            comprehendResponse = client.detect_entities(Text=description, LanguageCode='en')

            # we estimate the number of speakers in the podcast by parsing people names from the episode summary
            speaker_list = []
            for i in range(len(comprehendResponse["Entities"])):
                entity = comprehendResponse["Entities"][i]

                # For every person mentioned in the description, increment the number of 
                # speakers. This is making the assumption that the episode text will
                # mention all the speakers and not include mentions to people that
                # are not in the podcast.
                # Is isn't critical that this number is correct, it is simply used to break
                # up the body of the podcast into smaller chunks. If the speaker detection
                # is inaccurate, it doesn't have a major impact on the functionality of
                # the system.
                if entity['Type'] == 'PERSON':
                    speaker_list.append(entity['Text'])
                # add to vocabulary if not already in there
                if entity['Type'] in vocabularyTypes and not entity['Text'] in vocabularyItems:
                    vocabularyItems.append(entity['Text'])

            duplicates = find_duplicate_person(speaker_list)
            for d in duplicates:
                speaker_list.remove(d)
            num_speakers = len(speaker_list)

            # If there is an envelope, the link will point to an audio file
            if envelope != None:
                episode_url = envelope.attrib['url']
                file_type = envelope.attrib["type"]
                episode_count += 1

                episode = {
                    'Episode': title.text,
                    'PodcastName': channelTitle.text,
                    'podcastUrl': episode_url,
                    'audioType': file_type,
                    'tags': keywords,
                    'speakers': num_speakers,
                    'speakerNames': speaker_list,
                    'status': 'PENDING',
                    'publishedTime': date_string,
                    'summary': description,
                    'sourceFeed': feed_url
                }

                logger.debug(json.dumps(episode, indent=2))

                if "dryrun" in event:
                    episode["dryrun"] = event["dryrun"]
                # Add this item to the collection
                retval.append(episode)

            if max_episodes_to_process is not None and episode_count >= max_episodes_to_process:
                break

    # handle errors
    except HTTPError, e:
        print("HTTP Error:", e.code, feed_url)
        raise InvalidInputError("Unable to download RSS feed: " + feed_url)
    except URLError, e:
        print("URL Error:", e.reason, feed_url)
        raise InvalidInputError("Unable to download RSS feed: " + feed_url)

    logger.info(json.dumps(retval, indent=2))

    # This connection can be pretty big and exceed the capacity of the Step Function state data, so we store it
    # in S3 instead and return a link to the S3 file.
    s3_client = boto3.client('s3')
    key = 'podcasts/episodelist/' + id_generator() + '.json'
    response = s3_client.put_object(
        Body=json.dumps({"maxConcurrentEpisodes": maxConcurrentEpisodes, "episodes": retval}, indent=2), Bucket=bucket,
        Key=key)

    event['episodes'] = {"status": 'RUNNING', "remainingEpisodes": episode_count, "bucket": bucket, "key": key}
    event['customVocabulary'] = vocabularyItems

    # Return the link to the episode JSON document and the custom vocabulary items.
    return event
