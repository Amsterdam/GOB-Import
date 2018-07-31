import os
import logging

config = {
    "connector": {},
    "reader": {},
    "converter": {},
    "publisher": {
        "connection_address": os.environ["MESSAGE_BROKER_ADDRESS"],
        "exchange": "gob.workflow"
    },
    "logger": {
        "connection_address": os.environ["MESSAGE_BROKER_ADDRESS"],
        "exchange": "gob.log",
    }
}

LOGFORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
LOGLEVEL = logging.INFO
