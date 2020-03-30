from __future__ import print_function
import json
import os
import re
import boto3
from botocore.client import Config
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import xml.etree.ElementTree as ET
import string
import random
import time

transcribe_client = boto3.client('transcribe')

# Transribe will return spoken numbers in their word form, so if the custom vocabulary
# has 'S3' it needs to be converted to S-Three
convertDigitToWord = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine']

s3_client = boto3.client('s3')

# Generates a random ID for the step function execution
def id_generator(size=8, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))     

# Lambda S3 function
def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    
    bucket = os.environ['BUCKET_NAME']

    vocabularyTerms = []

    mapping = {}

    # Process each item in the vocabulary
    for i in range(len(event['customVocabulary'])): 
        row = event['customVocabulary'][i]

        # The items are comma separated, so split them apart.
        items = row.split(",")
        for j in range(len(items)): 
            # strip removes any whitespace leading or training the word
            origItem = items[j].strip()
            item = ""

            # Some string manipulation here to make the string format match
            # what Amazon Transcribe is expecting.
            # Numbers will be replaced with the text of the number. An enhancement
            # will be to include numbers greater than 9. The code here will split them
            # into each number, so Route 53 becomes Route-Five-Three, which isn't ideal.
            # Spaces in the string need to be replaced with dashes
            # The '.' is replaced with the word dot.
            # This loop goes right to left, starting at the end of the word and 
            # working to the front.
            for k in range(len(origItem)-1,-1,-1):
                letter = origItem[k]

                if k > 0 and origItem[k].isupper() and not origItem[k-1].isspace():
                    letter = '-' + letter

                if letter.isdigit():
                    letter = convertDigitToWord[int(letter)]
                    if k > 0:
                        letter = '-' + letter

                if letter == '.':
                    letter = '-dot'

                if letter.isspace():
                    letter = '-'

                if item == '' or item.startswith('-'):
                    while letter.endswith('-'):
                        letter = letter[:-1]

                # Remove any unsupported characters
                letter = re.sub(r'[^(a-z)(A-Z)-]','',letter)

                item = letter + item                

            mapping[item] = origItem
            vocabularyTerms.append(item)

    vocabularyName = id_generator()

    # Create the vocabulary
    response = transcribe_client.create_vocabulary(
        VocabularyName=vocabularyName,
        LanguageCode='en-US',
        Phrases=vocabularyTerms
    )
    print('created vocabulary:' + vocabularyName)


    mappingKey = 'podcasts/vocabularyMapping/' + id_generator() + '.json'
    s3_response = s3_client.put_object(Body= json.dumps(mapping, indent=2), Bucket= bucket, Key=mappingKey)


    return {
        "status": response['VocabularyState'],
        "name": vocabularyName,
        "mapping": {
            "bucket": bucket,
            "key": mappingKey
        }
    }

# Lambda S3 function
def check_vocabulary_status(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    response = transcribe_client.get_vocabulary(
        VocabularyName=event['name']
    )
    
    print(response)

    event['status'] = response['VocabularyState']

    return event

# Lambda S3 function
def delete_vocabulary(event, context):
    print("Delete Received event: " + json.dumps(event, indent=2))

    response = transcribe_client.delete_vocabulary(
        VocabularyName=event['name']
    )
    # response = s3_client.delete_object(Bucket= event['mapping']['bucket'], Key=event['mapping']['key'])

    
    return event