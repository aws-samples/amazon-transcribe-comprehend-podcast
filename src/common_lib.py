import logging
import random
import string

logging.basicConfig()
logger = logging.getLogger()


def find_duplicate_person(people):
    duplicates = []
    for i, person in enumerate(people):
        for j in range(i + 1, len(people)):
            if person in people[j]:
                if person not in duplicates:
                    duplicates.append(person)
                logger.info("found " + person + " in " + people[j])
            if people[j] in person:
                logger.info("found " + people[j] + " in " + person)
                if people[j] not in duplicates:
                    duplicates.append(people[j])
    return duplicates


# Creates a random string for file name
def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))
