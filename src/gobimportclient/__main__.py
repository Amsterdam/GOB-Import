"""Main program structure for an import client

Requires a dataset description-file to run:

     python -m gobimportclient example/meetbouten.json

:return: None
"""
import argparse

from gobcore.log import get_logger
from gobcore.model import GOBModel

from gobimportclient.import_client import ImportClient

logger = get_logger(name=__name__)

# Parse the arguments to get the import directory, e.g. /download
parser = argparse.ArgumentParser(description='Import datasource.')
parser.add_argument('datasource_description',
                    nargs='+', type=str,
                    help='the name of the datasource description (`meetbout`')
args = parser.parse_args()

gob_model = GOBModel()
for input_name in args.datasource_description:

    logger.info("Reading dataset description")

    logger.info(f"Import dataset {input_name} starts")

    import_client = ImportClient(dataset=gob_model.get_model)

    try:
        import_client.connect()
        import_client.read()
        import_client.convert()
        import_client.publish()
        logger.info(f"Import dataset {input_name} ended")
    except Exception as e:
        logger.error(f"Import dataset {input_name} failed: {str(e)}")
