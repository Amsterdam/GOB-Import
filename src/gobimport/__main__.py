"""Import

This component imports data sources
"""
import json
import logging
import sys

from gobconfig.import_.import_config import get_import_definition
from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.config import IMPORT_OBJECT_QUEUE, IMPORT_OBJECT_RESULT_KEY, IMPORT_QUEUE, \
    IMPORT_RESULT_KEY, WORKFLOW_EXCHANGE
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobimport.converter import MappinglessConverterAdapter
from gobimport.import_client import ImportClient
from argparse import ArgumentParser


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

    msg['header'] |= {
        'source': dataset['source']['name'],
        'application': dataset['source']['application'],
        'catalogue': dataset['catalogue'],
        'entity': dataset['entity'],
    }

    logger.configure(msg, "IMPORT")
    header = msg.get('header', {})

    # Create a new import client and start the process
    mode = ImportMode(header.get('mode', ImportMode.FULL.value))

    import_client = ImportClient(dataset=dataset, msg=msg, mode=mode, logger=logger)
    return import_client.import_dataset()


def handle_import_args(catalogue: str, collection: str, application: str, mode: str) -> dict:
    msg = {
        "header": {
            "catalogue": catalogue,
            "collection": collection,
            "application": application,
            "mode": mode
        }
    }
    dataset = extract_dataset_from_msg(msg)

    msg['header'] |= {
        'source': dataset['source']['name'],
        'application': dataset['source']['application'],
        'catalogue': dataset['catalogue'],
        'entity': dataset['entity']
    }

    logger.configure(msg, "IMPORT", logging.StreamHandler(stream=sys.stdout))

    mode = ImportMode(msg["header"].get('mode', ImportMode.FULL.value))
    import_client = ImportClient(dataset=dataset, msg=msg, mode=mode, logger=logger)

    dest = f"{msg['header']['catalogue']}/{msg['header']['entity']}"
    result_msg = import_client.import_dataset(destination=dest)

    logger.info(json.dumps(result_msg, indent=2))

    return result_msg


def handle_import_object_msg(msg):
    logger.configure(msg, "IMPORT OBJECT")
    logger.info("Start import object")
    importer = MappinglessConverterAdapter(msg['header'].get('catalogue'), msg['header'].get('entity'),
                                           msg['header'].get('entity_id_attr'))
    entity = importer.convert(msg['contents'])

    return {
        'header': {
            **msg['header'],
            'mode': ImportMode.SINGLE_OBJECT.value,
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


def get_parser():
    parser = ArgumentParser(
        prog="gobimport",
        description="Import a dataset from a registration.",
        epilog='Datateam Basis- en Kernregistraties'
    )
    parser.add_argument(
        "catalogue",
        type=str,
        help="The name of the data catalogue (example: \"meetbouten\")",
    )
    parser.add_argument(
        "collection",
        type=str,
        help="The name of the data collection (example: \"metingen\")",
    )
    parser.add_argument(
        "application",
        type=str,
        help="The name of the application to import from (default empty)"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "recent", "delete"],
        type=str,
        help="The import mode: full (default), recent or delete",
        required=False,
        default="full"
    )
    return parser


def init():
    if __name__ == "__main__":
        if len(sys.argv) == 1:
            print("No arguments found, wait for messages on the message broker.")
            messagedriven_service(SERVICEDEFINITION, "Import")

        else:
            print("Arguments found, start in stand-alone mode.")
            parser = get_parser()
            import_args = parser.parse_args()
            handle_import_args(**vars(import_args))


init()
