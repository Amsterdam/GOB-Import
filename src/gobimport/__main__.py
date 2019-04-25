"""Import

This component imports data sources
"""
from gobcore.message_broker import publish
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, IMPORT_QUEUE, RESULT_QUEUE
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobimport.import_client import ImportClient
from gobimport.mapping import get_mapping


def handle_import_msg(msg):
    dataset_file = msg['dataset_file'] if 'dataset_file' in msg else msg.get('header').get('dataset_file')
    assert dataset_file

    dataset = get_mapping(dataset_file)

    # Create a new import client and start the process
    import_client = ImportClient(dataset=dataset, msg=msg)
    msg = import_client.import_dataset()
    publish(RESULT_QUEUE, "import.result", msg)


SERVICEDEFINITION = {
    'import_request': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': IMPORT_QUEUE,
        'key': "import.start",
        'handler': handle_import_msg
    }
}


def init():
    if __name__ == "__main__":
        messagedriven_service(SERVICEDEFINITION, "Import")


init()
