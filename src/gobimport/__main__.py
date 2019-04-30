"""Import

This component imports data sources
"""
from gobcore.exceptions import GOBException
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, IMPORT_QUEUE, RESULT_QUEUE
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobimport.import_client import ImportClient
from gobimport.mapping import get_dataset_file_location, get_mapping


def extract_dataset_from_msg(msg):
    """Returns location of dataset file from msg.

    Dataset files can be identified by the file location ('dataset_file') or dataset identifier ('dataset') in the root
    of the message or in the 'contents' part of the message:

    message = { "dataset_file": "data/somedataset.json" }
    OR
    message = { "dataset": "some:dataset:identifier" }
    OR
    message = { "contents": { "dataset_file": "data/somedataset.json" } }
    OR
    message = { "contents": { "dataset": "some:dataset:identifier" } }

    :param msg:
    :return:
    """
    if 'dataset_file' in msg:
        return msg['dataset_file']
    elif msg.get('contents') and msg['contents'].get('dataset_file'):
        return msg['contents']['dataset_file']
    elif 'dataset' in msg:
        return get_dataset_file_location(msg['dataset'])
    elif msg.get('contents') and msg['contents'].get('dataset'):
        return get_dataset_file_location(msg['contents']['dataset'])
    else:
        raise GOBException('Missing dataset file')


def handle_import_msg(msg):
    dataset_file = extract_dataset_from_msg(msg)
    dataset = get_mapping(dataset_file)

    # Create a new import client and start the process
    import_client = ImportClient(dataset=dataset, msg=msg)
    return import_client.import_dataset()


SERVICEDEFINITION = {
    'import_request': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': IMPORT_QUEUE,
        'key': "import.start",
        'handler': handle_import_msg,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'queue': RESULT_QUEUE,
            'key': 'import.result'
        }
    }
}


def init():
    if __name__ == "__main__":
        messagedriven_service(SERVICEDEFINITION, "Import")


init()
