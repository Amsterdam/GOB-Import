"""Import

This component imports data sources
"""
import datetime

from gobconfig.import_.import_config import get_import_definition
from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.config import IMPORT, IMPORT_OBJECT_QUEUE, IMPORT_OBJECT_RESULT_KEY, IMPORT_QUEUE, \
    IMPORT_RESULT_KEY, WORKFLOW_EXCHANGE
from gobcore.message_broker.initialise_queues import create_queue_with_binding
from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobcore.workflow.start_workflow import start_workflow

from gobimport.config import MUTATIONS_IMPORT_CALLBACK_KEY, MUTATIONS_IMPORT_CALLBACK_QUEUE
from gobimport.converter import MappinglessConverterAdapter
from gobimport.database.connection import connect
from gobimport.database.repository import MutationImportRepository
from gobimport.database.session import DatabaseSession
from gobimport.import_client import ImportClient
from gobimport.mutations.exception import NothingToDo
from gobimport.mutations.handler import MutationsHandler


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

    if MutationsHandler.is_mutations_import(dataset):
        """The dataset source is marked as a mutations import. Let the MutationsHandler decide what to import and
        which mode to use.

        MutationsHandler returns a new MutationsImport object and the updated dataset configuration to use for this
        import
        """
        mutations_handler = MutationsHandler(dataset)
        logger.info("Have mutations import. Determine next step")

        with DatabaseSession() as session:
            repo = MutationImportRepository(session)
            last_import = repo.get_last(header.get('catalogue'), header.get('entity'), dataset.get('source', {})
                                        .get('application'))
            # If BAGExtract, create ImportClient with edited dataset (read_config) and mode. Add callback to msg header
            try:
                new_import, updated_dataset = mutations_handler.get_next_import(last_import)
            except NothingToDo as e:
                logger.error(f"Nothing to do: {e}")
                msg['summary'] = logger.get_summary()
                return msg

            repo.save(new_import)
            logger.info(f"File to be imported is {new_import.filename}")
            dataset = updated_dataset
            mode = ImportMode(new_import.mode)

            msg['header'] |= {
                'on_workflow_complete': {
                    'exchange': WORKFLOW_EXCHANGE,
                    'key': MUTATIONS_IMPORT_CALLBACK_KEY,
                },
                'mutation_import_id': new_import.id,
                'mode': mode.value,
            }
    else:
        # Create a new import client and start the process
        mode = ImportMode(header.get('mode', ImportMode.FULL.value))

    import_client = ImportClient(dataset=dataset, msg=msg, mode=mode, logger=logger)
    return import_client.import_dataset()


def handle_mutation_import_callback(msg):
    logger.configure(msg, "MUTATION_IMPORT_CALLBACK")
    dataset = extract_dataset_from_msg(msg)

    mutation_handler = MutationsHandler(dataset)
    with DatabaseSession() as session:
        # Mark import as ended
        repo = MutationImportRepository(session)
        mutation_import = repo.get(msg['header']['mutation_import_id'])
        mutation_import.ended_at = datetime.datetime.utcnow()
        repo.save(mutation_import)

        logger.info("Mutation import ended. Saving state in database")

        # Decide whether to start a next import or not
        if mutation_handler.have_next(mutation_import):
            logger.info("Have pending next import. Triggering new import.")
            copy_header = [
                'catalogue',
                'entity',
                'collection',
                'application',
            ]
            start_workflow({
                'workflow_name': IMPORT,
            }, {k: msg['header'][k] for k in msg['header'].keys() if k in copy_header})
        else:
            logger.info("This was the last file to be imported for now.")


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
    'mutations_import_callback': {
        'queue': MUTATIONS_IMPORT_CALLBACK_QUEUE,
        'handler': handle_mutation_import_callback,
    }
}


def init():
    if __name__ == "__main__":
        connect()
        create_queue_with_binding(
            exchange=WORKFLOW_EXCHANGE,
            queue=MUTATIONS_IMPORT_CALLBACK_QUEUE,
            key=MUTATIONS_IMPORT_CALLBACK_KEY
        )
        messagedriven_service(SERVICEDEFINITION, "Import")


init()
