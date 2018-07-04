"""Main program structure for an import client

:return: None
"""
from client.config import config

from client.connector import connect
from client.reader import read
from client.converter import convert
from client.publisher import publish
from client.log import get_logger


logger = get_logger(config=config["logger"], name=__name__)
logger.info("Import start")

connection = connect(config=config["connector"])
raw_data = read(config=config["reader"], connection=connection)
data = convert(config=config["converter"], raw_data=raw_data)
publish(config=config["publisher"], data=data)

logger.info("Import end")
