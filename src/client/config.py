import os
import logging

config = {
    "connector": {},
    "reader": {},
    "converter": {},
    "publisher": {
        "connection_address": os.environ["MESSAGE_BROKER_ADDRESS"],
        "queue": "gob.workflow"
    },
    "logger": {
        "connection_address": os.environ["MESSAGE_BROKER_ADDRESS"],
        "queue": "gob.log",
    }
}

LOGFORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
LOGLEVEL = logging.WARNING
