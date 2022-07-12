"""Import

This component imports data sources
"""
import sys

from gobconfig.import_.import_config import get_import_definition
from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.config import IMPORT_OBJECT_QUEUE, IMPORT_OBJECT_RESULT_KEY, IMPORT_QUEUE, \
    IMPORT_RESULT_KEY, WORKFLOW_EXCHANGE
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


def handle_import_msg(msg: dict, use_message_broker: bool = True, destination: str = None) -> dict:
    """
    Handles the message based on either:
     - Input from arguments given to main()
     - Message on the message broker queue

    :param msg: valid (import) message
    :param use_message_broker: Log to the message broker if true
    :param destination: optional destination on GOB_SHARED_DIR
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

    if use_message_broker:
        logger.add_message_broker_handler()

    header = msg.get('header', {})

    # Create a new import client and start the process
    mode = ImportMode(header.get('mode', ImportMode.FULL.value))

    import_client = ImportClient(dataset=dataset, msg=msg, mode=mode, logger=logger)
    return import_client.import_dataset(destination)


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


def main():
    if len(sys.argv) == 1:
        print("No arguments found, wait for messages on the message broker.")
        messagedriven_service(SERVICEDEFINITION, "Import")

    else:
        print("Arguments found, start in stand-alone mode.")

        args = WorkflowCommands(["import"]).parse_arguments()
        dest = "/".join(dst for dst in [args.get("catalogue"), args.get("collection")] if dst)

        # TODO handle the return message
        print(handle_import_msg(msg={"header": args}, use_message_broker=False, destination=dest))


if __name__ == "__main__":
    main()  # pragma: no cover
