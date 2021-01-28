"""Import

This component imports data sources
"""
from gobconfig.import_.import_config import get_import_definition
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.config import IMPORT_OBJECT_QUEUE, IMPORT_OBJECT_RESULT_KEY, IMPORT_QUEUE, \
    IMPORT_RESULT_KEY, WORKFLOW_EXCHANGE
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobimport.config import FULL_IMPORT, SINGLE_OBJECT_IMPORT
from gobimport.converter import MappinglessConverterAdapter
from gobimport.import_client import ImportClient


def extract_dataset_from_msg(msg):
    """Returns location of dataset file from msg.

    Example message:

    message = {
       "header": {
          "catalogue": "some catalogue",
          "collection": "the collection",
          "application": "the application"
       }
    }

    Where 'application' is optional when there is only one known application for given catalogue and collection

    :param msg:
    :return:
    """

    required_keys = ['catalogue', 'collection']
    header = msg.get('header', {})

    if not all([key in header for key in required_keys]):
        raise GOBException(f"Missing dataset keys. Expected keys: {','.join(required_keys)}")

    return get_import_definition(header['catalogue'], header['collection'], header.get('application'))


def handle_import_msg(msg):
    dataset = extract_dataset_from_msg(msg)

    # Create a new import client and start the process
    header = msg.get('header', {})
    mode = header.get('mode', FULL_IMPORT)
    import_client = ImportClient(dataset=dataset, msg=msg, mode=mode)
    return import_client.import_dataset()


def handle_import_object_msg(msg):
    logger.configure(msg, "IMPORT OBJECT")
    logger.info("Start import object")
    importer = MappinglessConverterAdapter(msg['header'].get('catalogue'), msg['header'].get('entity'),
                                           msg['header'].get('entity_id_attr'))
    entity = importer.convert(msg['contents'])

    return {
        'header': {
            **msg['header'],
            'mode': SINGLE_OBJECT_IMPORT,
            'collection': msg['header'].get('entity'),
        },
        'summary': logger.get_summary(),
        'contents': [entity]
    }


SERVICEDEFINITION = {
    'import_request': {
        'queue': IMPORT_QUEUE,
        'handler': handle_import_msg,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': IMPORT_RESULT_KEY,
        },
    },
    'import_single_object_request': {
        'queue': IMPORT_OBJECT_QUEUE,
        'handler': handle_import_object_msg,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': IMPORT_OBJECT_RESULT_KEY,
        },
    },
}


def init():
    if __name__ == "__main__":
        messagedriven_service(SERVICEDEFINITION, "Import")


init()
