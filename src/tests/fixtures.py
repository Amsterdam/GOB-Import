import random
import string


def random_string(length=12, source=None):
    if source is None:
        source = string.ascii_letters

    return ''.join(random.choice(source) for x in range(length))


def get_valid_meetbouten():
    return [{
        'identificatie': random_string(8, ['0','1','2','3','4','5','6','7','8','9']),
        'status_id': random_string(1, ['1','2','3']),
        'windrichting': random.choice(['N','NO','O','ZO','Z','ZW','W','NW']),
        'publiceerbaar': random.choice(['1', '0']),
    }]

def get_valid_entity_with_state():
    return [{
        'identificatie': random_string(8, ['0','1','2','3','4','5','6','7','8','9']),
        'volgnummer': random_string(1, ['0','1','2','3','4','5','6','7','8','9'])
    }]

def get_fatal_meetbouten():
    return [{
        'identificatie': random_string(),
        'status_id': random_string(1, ['1','2','3']),
        'windrichting': random.choice(['N','NO','O','ZO','Z','ZW','W','NW']),
        'publiceerbaar': random.choice(['1', '0']),
    }]

def get_invalid_meetbouten():
    return [{
        'identificatie': random_string(8, ['0','1','2','3','4','5','6','7','8','9']),
        'status_id': '4',
        'windrichting': random.choice(['N','NO','O','ZO','Z','ZW','W','NW']),
        'publiceerbaar': random.choice(['1', '0']),
    }]

def get_nopubliceerbaar_meetbouten():
    return [{
        'identificatie': random_string(8, ['0','1','2','3','4','5','6','7','8','9']),
        'status_id': random_string(1, ['1','2','3']),
        'windrichting': random.choice(['N','NO','O','ZO','Z','ZW','W','NW']),
        'publiceerbaar': '2',
    }]

def get_nullpubliceerbaar_meetbouten():
    return [{
        'identificatie': random_string(8, ['0','1','2','3','4','5','6','7','8','9']),
        'status_id': random_string(1, ['1','2','3']),
        'windrichting': random.choice(['N','NO','O','ZO','Z','ZW','W','NW']),
        'publiceerbaar': None,
    }]
