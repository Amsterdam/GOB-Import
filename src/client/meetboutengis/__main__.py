import argparse

from client.config import config
from client.log import get_logger
from client.meetboutengis.meetbouten import Meetbouten

logger = get_logger(config=config["logger"], name="meetboutengis import")

parser = argparse.ArgumentParser(description='Import meetboutengis datasources.')
parser.add_argument('datasource_dir',
                    type=str,
                    help='the datasource directory to import')
args = parser.parse_args()
input_dir = args.datasource_dir

sources = [
    Meetbouten(config=config, input_dir=input_dir),
    # Metingen
    # ...
]


for source in sources:
    logger.info(f"{source.id()} Start")
    try:
        source.connect()
        source.read()
        source.convert()
        source.publish()
        logger.info(f"{source.id()} End")
    except Exception as e:
        logger.error(f"{source.id()} Failed")
