import os
import logging

config = {
    "connector": {},
    "reader": {},
    "converter": {},
    "publisher": {
        "connection_address": os.environ["MESSAGE_BROKER_ADDRESS"],
        "queue": "gob.workflow"
    }
}

LOGLEVEL = logging.WARNING
