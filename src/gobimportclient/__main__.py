"""Main program structure for an import gobimportclient

:return: None
"""
from gobimportclient.config import config

from gobimportclient.connector import connect
from gobimportclient.reader import read
from gobimportclient.converter import convert
from gobimportclient.publisher import publish
from gobimportclient.log import get_logger


logger = get_logger(config=config["logger"], name=__name__)
logger.info("Import start")

connection = connect(config=config["connector"])
raw_data = read(config=config["reader"], connection=connection)
data = convert(config=config["converter"], raw_data=raw_data)
publish(config=config["publisher"], data=data)

logger.info("Import end")
