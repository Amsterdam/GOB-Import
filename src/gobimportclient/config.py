"""Configuration for an import client

This module contains the configuration for the data import.

Two environment variables are used:
    MESSAGE_BROKER_ADDRESS - The address of the message broker (localhost when running locally)
    LOCAL_DATADIR - The directory in which any input data files are located (default src/data)

"""
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

LOCAL_DATADIR = os.path.join(os.path.dirname(__file__), '..', 'example', 'data')
LOCAL_DATADIR = os.getenv('LOCAL_DATADIR', LOCAL_DATADIR)
