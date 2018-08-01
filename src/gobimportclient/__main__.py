"""Main program structure for an import client

Requires a dataset description-file to run:

     python -m gobimportclient example/meetbouten.json

:return: None
"""
import argparse
import json
import os

from jsonschema import validate

from gobimportclient.config import config
from gobimportclient.import_client import ImportClient
from gobimportclient.log import get_logger

SCHEMA_FILE = 'dataset_schema.json'

logger = get_logger(config=config["logger"], name=__name__)

# Parse the arguments to get the import directory, e.g. /download
parser = argparse.ArgumentParser(description='Import datasource.')
parser.add_argument('datasource_description',
                    nargs='+', type=str,
                    help='the datasource description (`meetbouten.json`')
args = parser.parse_args()

for input_file in args.datasource_description:

    logger.info("Reading dataset description")

    with open(input_file) as file:
        dataset = json.load(file)

    logger.info(f"Import dataset {dataset['entity']} starts")

    import_client = ImportClient(config=config, dataset=dataset)

    import_client.connect()
    import_client.read()
    import_client.convert()
    import_client.publish()
    logger.info(f"Import dataset {dataset['entity']} ended")

    # try:
    #     import_client.connect()
    #     import_client.read()
    #     import_client.convert()
    #     import_client.publish()
    #     logger.info(f"Import dataset {dataset['entity']} ended")
    # except Exception as e:
    #     logger.error(f"Import dataset {dataset['entity']} failed: {str(e)}")
