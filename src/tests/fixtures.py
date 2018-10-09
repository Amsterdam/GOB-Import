import random
import string


def random_string(length=12, source=None):
    if source is None:
        source = string.ascii_letters

    return ''.join(random.choice(source) for x in range(length))


def get_valid_meetbouten():
    return [{
        'meetboutid': random_string(8, ['0','1','2','3','4','5','6','7','8','9']),
    }]


def get_invalid_meetbouten():
    return [{
        'meetboutid': random_string(),
    }]
