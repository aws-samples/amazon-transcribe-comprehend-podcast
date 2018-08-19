from __future__ import print_function
import json
import os
import boto3
import string
import random

s3_client = boto3.client('s3')


# Generates a random ID for the step function execution
def id_generator(size=32, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


# Entry point of the lamnda function
def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    client = boto3.client('stepfunctions')

    isDebug = False

    # Pull the location of the episode list form S3 and parse the JSON
    response = s3_client.get_object(Bucket=event["episodes"]['bucket'], Key=event["episodes"]['key'])
    file_content = response['Body'].read().decode('utf-8')

    request = json.loads(file_content)
    maxConcurrentEpisodes = request["maxConcurrentEpisodes"]
    episodes = request["episodes"]

    results = []

    # Pull the step function arn from the environment variables set by the cloudformation script
    stepFunctionArn = os.environ['STEP_FUNCTION_ARN']

    runningExecutions = 0
    remainingEpisodes = 0

    # for each episode, run the step function for an individual episode
    # Keep track of the number of running episodes. In order to prvent too
    # many concurrent calls to transcribe, there will be a limit on the 
    # number of concurrent executions. This loop checks all the RUNNING
    # episodes and gets a status update from Amazon Transcribe.
    for i in range(len(episodes)):
        episode = episodes[i]

        if episode["status"] == "RUNNING":
            # get the status of the execution
            response = client.describe_execution(executionArn=episode['executionArn'])
            episode['status'] = response['status']

            if episode["status"] == "RUNNING":
                runningExecutions += 1

        if episode["status"] == "RUNNING" or episode["status"] == "PENDING":
            remainingEpisodes += 1

    # for each episode, run the step function for an individual episode
    # Throttle the number of conncurent executions
    for i in range(len(episodes)):
        episode = episodes[i]

        if runningExecutions >= maxConcurrentEpisodes:
            break

        if episode["status"] == 'PENDING':
            episodeRequest = {
                "Episode": episode['Episode'],
                "PodcastName": episode['PodcastName'],
                "dryrun": episode['dryrun'],
                "tags": episode['tags'],
                "podcastUrl": episode['podcastUrl'],
                "speakers": episode['speakers'],
                "bucket": event["episodes"]['bucket'],
                "publishTime": episode['publishedTime'],
                "audio_type": episode['audioType'],
                "summary": episode['summary'],
                "sourceFeed": episode['sourceFeed'],
                "vocabularyInfo": {
                    "name": event["vocabularyInfo"]['name'],
                    "mapping": event["vocabularyInfo"]['mapping']
                }
            }
            if 'speakerNames' in episode:
                episodeRequest['speakerNames'] = episode['speakerNames']

            print("Calling Child Step Function: " + json.dumps(episodeRequest, indent=4, sort_keys=True, default=str))

            response = client.start_execution(
                stateMachineArn=stepFunctionArn,
                name=id_generator(),
                input=json.dumps(episodeRequest, indent=4, sort_keys=True, default=str)
            )

            # Create an execution of the child step function
            episode["executionArn"] = response["executionArn"]
            episode["status"] = "RUNNING"
            runningExecutions += 1

    feedStatus = "RUNNING"
    # If the remainingEpisodes count is 0, then the processing is complete
    if remainingEpisodes == 0:
        feedStatus = "COMPLETE"

    # The execution list can get long, so we store it in s3 as to not exceed max for the payload of the
    # step function.
    key = 'podcasts/keywords/' + id_generator() + '.json'
    if feedStatus != "COMPLETE":
        response = s3_client.put_object(Body=json.dumps(request, indent=2), Bucket=event["episodes"]['bucket'], Key=key)

    # Delete the prior item
    response = s3_client.delete_object(Bucket=event["episodes"]['bucket'], Key=event["episodes"]['key'])

    if isDebug:
        print("REQUEST:")
        print(json.dumps(request, indent=2))

    # return the execution status of the child step functions
    return {"status": feedStatus, "remainingEpisodes": remainingEpisodes, "bucket": event["episodes"]['bucket'],
            "key": key}
