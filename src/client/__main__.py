from config import config

from connector import connect
from reader import read
from converter import convert
from publisher import publish
from log import get_logger


def main():
    '''
    Main program structure for an import client

    :return: None
    '''
    logger = get_logger(__name__)
    logger.info("Import start")

    connection = connect(config=config["connector"])
    raw_data = read(config=config["reader"], connection=connection)
    data = convert(config=config["converter"], raw_data=raw_data)
    publish(config=config["publisher"], data=data)

    logger.info("Import end")


if __name__ == "__main__":
    main()
