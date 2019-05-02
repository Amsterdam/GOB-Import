"""Import

This component imports data sources
"""
from gobcore.exceptions import GOBException
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, IMPORT_QUEUE, RESULT_QUEUE
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobimport.import_client import ImportClient
from gobimport.mapping import get_dataset_file_location, get_mapping


def _extract_dataset_variable(dataset):
    """Returns dataset file from dataset variable. If dataset is a string it is assumed to be a file path. If dataset
    is a dict we perform a lookup to get the correct file path.

    :param dataset:
    :return:
    """
    if isinstance(dataset, str):
        return dataset
    elif isinstance(dataset, dict):
        keys = ['catalogue', 'collection', 'application']
        if not all([key in dataset for key in keys]):
            raise GOBException(f"Missing dataset keys. Expected keys: {','.join(keys)}")
        return get_dataset_file_location(dataset['catalogue'], dataset['collection'], dataset['application'])
    else:
        raise GOBException("Dataset of invalid type. Expecting str or dict")


def extract_dataset_from_msg(msg):
    """Returns location of dataset file from msg.

    The dataset to import should be defined either in the root of the msg or in the 'contents' part of the msg.
    The dataset can be defined as string (the dataset location) or as dictionary (with catalogue, collection and
    application as keys), in which case we look up the correct file in the available datasets.

    message = { "dataset": "data/somedataset.json" }
    OR
    message = {
    "dataset": {
       "catalogue": "some catalogue",
       "collection": "the collection",
       "application": "the application"
        }
    }
    OR
    message = { "contents": { "dataset": "data/somedataset.json" } }
    OR
    message = {
    "contents": {
        "dataset": {
            "catalogue": "some catalogue",
            "collection": "the collection",
            "application": "the application"
            }
        }
    }

    :param msg:
    :return:
    """

    if 'dataset' in msg:
        return _extract_dataset_variable(msg['dataset'])
    elif msg.get('contents') and msg['contents'].get('dataset'):
        return _extract_dataset_variable(msg['contents']['dataset'])
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
