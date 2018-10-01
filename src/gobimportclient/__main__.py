"""Main program structure for an import client

The import client subscribes to the workflow queue if started without arguments.

Requires a dataset description-file to run an import:

     python -m gobimportclient example/meetbouten.json

:return: None

"""
import argparse

from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobcore.log import get_logger

from gobimportclient.import_client import ImportClient
from gobimportclient.mapping import get_mapping

# If we have args we are expected to process the datafile
# TODO: Listen to the correct events in the queue, instead of manual trigger

logger = get_logger(name=__name__)

# Parse the arguments to get the import directory, e.g. /download
parser = argparse.ArgumentParser(description='Import datasource.')
parser.add_argument('datasource_description',
                    nargs='*', type=str,
                    help='the name of the datasource description (`meetbout`')
args = parser.parse_args()

if len(args.datasource_description) > 0:
    # If we receive a datasource as an argument, start processing the batch
    for input_name in args.datasource_description:

        logger.info("Reading dataset description")

        logger.info(f"Import dataset {input_name} starts")

        import_client = ImportClient(dataset=get_mapping(input_name))

        try:
            import_client.connect()
            import_client.read()
            import_client.convert()
            import_client.publish()
            logger.info(f"Import dataset {input_name} ended")
        except Exception as e:
            logger.error(f"Import dataset {input_name} failed: {str(e)}")

else:
    # Start message driven service to keep the docker alive
    SERVICEDEFINITION = {
        'dataimport.proposal': {
            'queue': "gob.workflow.proposal",
            # for now only pass through the message-content:
            'handler': lambda msg: msg,
            'report_back': 'dataimport.request',
            'report_queue': 'gob.workflow.request'
        },
    }

    messagedriven_service(SERVICEDEFINITION)
