"""Import.

This component imports data sources.
"""


import argparse
import sys
from typing import Any

from gobconfig.import_.import_config import get_import_definition
from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.config import (
    IMPORT_OBJECT_QUEUE,
    IMPORT_OBJECT_RESULT_KEY,
    IMPORT_QUEUE,
    IMPORT_RESULT_KEY,
    WORKFLOW_EXCHANGE,
)
from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobcore.message_broker.typing import ServiceDefinition
from gobcore.standalone import parent_argument_parser, run_as_standalone

from gobimport.converter import MappinglessConverterAdapter
from gobimport.import_client import DatasetMappingType, ImportClient


def argument_parser() -> argparse.ArgumentParser:
    """Parse arguments for the import handler in standalone mode."""
    parser: argparse.ArgumentParser
    parser, subparsers = parent_argument_parser()

    # import handler parser
    import_parser = subparsers.add_parser(name="import", description="Start an import job for a collection")
    import_parser.add_argument(
        "--catalogue", required=True, help='The name of the data catalogue (example: "meetbouten")'
    )
    import_parser.add_argument(
        "--collection",
        required=True,
        help='The name of the data collection (example: "metingen"), ' "also known as 'entity'.",
    )
    import_parser.add_argument("--application", required=False, help="The name of the application to import from")
    import_parser.add_argument(
        "--mode",
        required=False,
        help="The mode to use. Defaults to full",
        default="full",
        choices=["delete", "full", "recent"],
    )
    return parser


def extract_dataset_from_msg(msg: dict[str, Any]) -> DatasetMappingType:
    """Return location of dataset file from msg.

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
    required_keys = ["catalogue", "collection"]
    header = msg.get("header", {})

    if not all([key in header for key in required_keys]):
        raise GOBException(f"Missing dataset keys. Expected keys: {','.join(required_keys)}")

    return get_import_definition(header["catalogue"], header["collection"], header.get("application"))


def handle_import_msg(msg: dict[str, Any]) -> dict[str, str]:
    """Handle an import message from the message broker queue.

    :param msg: valid (import) message
    :return: result msg
    """
    dataset = extract_dataset_from_msg(msg)
    msg["header"] |= {
        "source": dataset["source"]["name"],
        "application": dataset["source"]["application"],
        "catalogue": dataset["catalogue"],
        "entity": dataset["entity"],
    }

    mode = ImportMode(msg["header"].get("mode", ImportMode.FULL.value))
    with ImportClient(dataset=dataset, msg=msg, mode=mode, logger=logger) as import_client:
        import_client.import_dataset()

    result: dict[str, str] = import_client.get_result_msg()
    return result


def handle_import_object_msg(msg: dict[str, Any]) -> dict[str, Any]:
    """Handle an import object message."""
    logger.info("Start import object")

    importer = MappinglessConverterAdapter(
        msg["header"].get("catalogue"), msg["header"].get("entity"), msg["header"].get("entity_id_attr")
    )
    entity = importer.convert(msg["contents"])

    return {
        "header": {
            **msg["header"],
            "mode": ImportMode.SINGLE_OBJECT.value,
            "collection": msg["header"].get("entity"),
        },
        "summary": logger.get_summary(),
        "contents": [entity],
    }


SERVICEDEFINITION: ServiceDefinition = {
    "import": {
        "queue": IMPORT_QUEUE,
        "handler": handle_import_msg,
        "report": {"exchange": WORKFLOW_EXCHANGE, "key": IMPORT_RESULT_KEY},
        "pass_args_standalone": [
            "mode",
        ],
    },
    "import_single_object_request": {
        "queue": IMPORT_OBJECT_QUEUE,
        "handler": handle_import_object_msg,
        "report": {"exchange": WORKFLOW_EXCHANGE, "key": IMPORT_OBJECT_RESULT_KEY},
    },
}


def main():
    """Determine import mode: messagedriven_service or standalone."""
    if len(sys.argv) == 1:
        print("No arguments found, wait for messages on the message broker.")
        messagedriven_service(SERVICEDEFINITION, "Import")

    else:
        print("Arguments found, start in standalone mode.")
        parser = argument_parser()
        args = parser.parse_args()
        # Configure import handler to raise an exception when running standalone.
        sys.exit(run_as_standalone(args, SERVICEDEFINITION))


if __name__ == "__main__":
    main()  # pragma: no cover
