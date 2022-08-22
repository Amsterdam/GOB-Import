"""Import

This component imports data sources
"""
import json

import sys
from gobcore.standalone import default_parser, run_as_standalone, write_message
from pathlib import Path

from gobconfig.import_.import_config import get_import_definition
from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.config import IMPORT_OBJECT_QUEUE, \
    IMPORT_OBJECT_RESULT_KEY, IMPORT_QUEUE, IMPORT_RESULT_KEY, WORKFLOW_EXCHANGE
from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobcore.workflow.start_commands import WorkflowCommands
from typing import Any, Callable, Dict

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

    Where 'application' is optional when there is only one known application for given
    catalogue and collection

    :param msg:
    :return:
    """

    required_keys = ['catalogue', 'collection']
    header = msg.get('header', {})

    if not all([key in header for key in required_keys]):
        raise GOBException(f"Missing dataset keys. Expected keys: {','.join(required_keys)}")

    return get_import_definition(header['catalogue'], header['collection'], header.get('application'))


def handle_import_msg(msg: dict) -> dict:
    """
    Handles an import message from the message broker queue.

    :param msg: valid (import) message
    :return: result msg
    """
    dataset = extract_dataset_from_msg(msg)
    msg['header'] |= {
        'source': dataset['source']['name'],
        'application': dataset['source']['application'],
        'catalogue': dataset['catalogue'],
        'entity': dataset['entity'],
    }

    logger.configure(msg, "IMPORT")
    logger.add_message_broker_handler()

    mode = ImportMode(msg["header"].get('mode', ImportMode.FULL.value))

    with ImportClient(dataset=dataset, msg=msg, mode=mode, logger=logger) as import_client:
        import_client.import_dataset()

    result_msg = import_client.get_result_msg()
    return result_msg


def handle_import_sa_msg(msg: dict[str, Any], args) -> dict[str, Any]:
    """Standalone variation of handle_import_msg.

    TODO: see if we can pass raise_exception as an argument. Maybe with a partial.
    """
    dest = Path(msg["header"].get("catalogue"), msg["header"].get("entity", ""))
    mode = ImportMode(args.pop("mode", ImportMode.FULL.value))
    dataset = extract_dataset_from_msg(msg)
    # Create a new import client and start the process
    # Move this to callback
    with ImportClient(dataset=dataset, msg=msg, mode=mode, logger=logger) as import_client:
        # This differs with handle_import_msg
        import_client.raise_exception = True
        import_client.import_dataset(str(dest))

    return import_client.get_result_msg()


def handle_import_object_msg(msg):
    logger.configure(msg, "IMPORT OBJECT")
    logger.add_message_broker_handler()

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


def _construct_message(args):
    msg = {"header": args}
    dataset = extract_dataset_from_msg(msg)
    msg["header"] |= {
        "source": dataset["source"]["name"],
        "application": dataset["source"]["application"],
        "catalogue": dataset["catalogue"],
        "entity": dataset["entity"],
    }
    msg["header"].pop("collection", None)  # collection == entity
    return msg


def _get_handler(args):
    return SERVICEDEFINITION.get(args.handler)["handler"]


SERVICEDEFINITION = {
    'import': {  # Alias for import_request to support 'gobimport import'
        'queue': IMPORT_QUEUE,
        'handler': handle_import_sa_msg,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': IMPORT_RESULT_KEY,
        },
    },
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


def main():
    if len(sys.argv) == 1:
        print("No arguments found, wait for messages on the message broker.")
        messagedriven_service(SERVICEDEFINITION, "Import")
    else:
        print("Arguments found, start in stand-alone mode.")
        default_args = default_parser(SERVICEDEFINITION)
        if default_args.message_data:
            run_as_standalone(
                args=default_args,
                handler=_get_handler(default_args),
                message_data=json.loads(default_args.message_data)
            )
        else:
            args = WorkflowCommands([default_args.handler]).parse_arguments()
            run_as_standalone(
                args=args,
                handler=_get_handler(args),
                message_data=_construct_message(args)
            )

        # TODO: Handle result message, process_issues


if __name__ == "__main__":
    main()  # pragma: no cover
