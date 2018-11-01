"""Main program structure for an import client

The import client subscribes to the workflow queue if started without arguments.

Requires a dataset description-file to run an import:

     python -m gobimport example/meetbouten.json

:return: None

"""
import argparse
import time

from gobimport.import_client import ImportClient
from gobimport.mapping import get_mapping

# If we have args we are expected to process the datafile
# TODO: Listen to the correct events in the queue, instead of manual trigger

# Parse the arguments to get the import directory, e.g. /download
parser = argparse.ArgumentParser(description='Import datasource.')
parser.add_argument('datasource_description',
                    nargs='*', type=str,
                    help='the name of the datasource description (`meetbout`')
args = parser.parse_args()

if len(args.datasource_description) > 0:
    # If we receive a datasource as an argument, start processing the batch
    for input_name in args.datasource_description:

        # Create a new import client and start the process
        import_client = ImportClient(dataset=get_mapping(input_name))
        import_client.start_import_process()

else:
    while True:
        print('.')
        time.sleep(60)
