"""Import

This component imports data sources
"""
import sys
from pathlib import Path

from gobconfig.import_.import_config import get_import_definition
from gobcore.datastore.xcom_data_store import XComDataStore
from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.config import IMPORT_OBJECT_QUEUE, \
    IMPORT_OBJECT_RESULT_KEY, IMPORT_QUEUE, IMPORT_RESULT_KEY, WORKFLOW_EXCHANGE
from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobcore.workflow.start_commands import WorkflowCommands

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


def run_as_standalone(args: dict):
    """
    Run gob-import as stand-alone application. Parses and processes the cli arguments to a result message.
    Logging is send to stdout.

    example: python -m gobimport import gebieden wijken DGDialog

    :return: result message
    """
    mode = ImportMode(args.pop("mode", ImportMode.FULL.value))

    msg = {"header": args}
    dataset = extract_dataset_from_msg(msg)

    msg["header"] |= {
        "source": dataset["source"]["name"],
        "application": dataset["source"]["application"],
        "catalogue": dataset["catalogue"],
        "entity": dataset["entity"],
    }
    msg["header"].pop("collection", None)  # collection == entity

    logger.configure(msg, "IMPORT")

    dest = Path(msg["header"].get("catalogue"), msg["header"].get("entity", ""))

    # Create a new import client and start the process
    with ImportClient(dataset=dataset, msg=msg, mode=mode, logger=logger) as import_client:
        import_client.raise_exception = True
        import_client.import_dataset(str(dest))

    result = import_client.get_result_msg()

    if full_path := result.get("contents_ref"):
        logger.info(f"Imported collection to: {full_path}")
        XComDataStore().write(result)

    return result


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


def main():
    if len(sys.argv) == 1:
        print("No arguments found, wait for messages on the message broker.")
        messagedriven_service(SERVICEDEFINITION, "Import")

    else:
        print("Arguments found, start in stand-alone mode.")
        args = WorkflowCommands(["import"]).parse_arguments()
        run_as_standalone(args)

        # TODO: Handle result message, process_issues


if __name__ == "__main__":
    main()  # pragma: no cover
