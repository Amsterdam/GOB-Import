"""ImportClient class

An ImportClient is instantiated using a configuration and dataset definition.
The configuration is shared between import clients and contains for instance the message broker to
publish the results
The dataset is specific for each import client and tells for instance which fields should be extracted

The current implementation assumes csv-file based imports

Todo: improve type conversion
    e.g. for bools the true and false values are hardcoded.
    N = False, else is True, but this can vary per import
"""

import datetime
import traceback

from gobcore.logging.logger import logger
from gobcore.utils import ProgressTicker

from gobcore.message_broker import publish
from gobcore.message_broker.offline_contents import ContentsWriter

from gobimport.reader import Reader
from gobimport.converter import Converter
from gobimport.injections import Injector
from gobimport.merger import Merger
from gobimport.validator import Validator
from gobimport.enricher import Enricher
from gobimport.entity_validator import EntityValidator


class ImportClient:
    """Main class for an import client

    This class serves as the main client for which the import can be configured in a dataset.json

    """
    def __init__(self, dataset, msg):
        self.header = msg.get("header", {})

        self.init_dataset(dataset)

        self.validator = Validator(self.entity, self.source_id)
        self.entity_validator = EntityValidator(self.catalogue, self.entity, self.func_source_id)
        self.merger = Merger(self)

        # Extra variables for logging
        start_timestamp = int(datetime.datetime.utcnow().replace(microsecond=0).timestamp())
        self.process_id = f"{start_timestamp}.{self.source_app}.{self.entity}"
        extra_log_kwargs = {
            'process_id': self.process_id,
            'source': self.source['name'],
            'application': self.source.get('application'),
            'catalogue': self.catalogue,
            'entity': self.entity
        }

        # Log start of import process
        logger.set_name("IMPORT")
        logger.set_default_args(extra_log_kwargs)
        logger.info(f"Import dataset {self.entity} from {self.source_app} started")

    def init_dataset(self, dataset):
        self.dataset = dataset
        self.source = self.dataset['source']
        self.source_id = self.dataset['source']['entity_id']
        self.source_app = self.dataset['source'].get('application', self.dataset['source']['name'])
        self.catalogue = self.dataset['catalogue']
        self.entity = self.dataset['entity']

        # Find the functional source id
        # This is the functional field that is mapped onto the source_id
        # or _source_id if no mapping exists
        ids = [key for key, value in self.dataset["gob_mapping"].items() if value["source_mapping"] == self.source_id]
        self.func_source_id = ids[0] if ids else "_source_id"

        self.injector = Injector(self.source.get("inject"))
        self.enricher = Enricher(self.catalogue, self.entity)
        self.converter = Converter(self.catalogue, self.entity, self.dataset)

    def publish(self):
        """The result of the import needs to be published.

        Publication includes a header, summary and results
        The header is for identification purposes
        The summary is for the interpretation of the results. Was the import successful, what er the metrics, etc
        The results is the imported data in GOB format

        :return:
        """
        metadata = {
            **self.header,
            "process_id": self.process_id,
            "source": self.dataset['source']['name'],
            "application": self.dataset['source'].get('application'),
            "depends_on": self.dataset['source'].get('depends_on', {}),
            "enrich": self.dataset['source'].get('enrich', {}),
            "catalogue": self.dataset['catalogue'],
            "entity": self.dataset['entity'],
            "version": self.dataset['version'],
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

        summary = {
            'num_records': self.n_rows
        }

        # Log end of import process
        logger.info(f"Import dataset {self.entity} from {self.source_app} completed. "
                    f"{summary['num_records']} records were read from the source.",
                    kwargs={"data": summary})

        import_message = {
            "header": metadata,
            "summary": summary,
            "contents_ref": self.filename
        }
        publish("gob.workflow.proposal", "fullimport.proposal", import_message)

    def import_data(self, write, progress):
        logger.info(f"Connect to {self.source_app}")
        reader = Reader(self.source, self.source_app)
        reader.connect()

        logger.info(f"Start import from {self.source_app}")
        self.n_rows = 0
        for row in reader.read():
            progress.tick()

            self.row = row
            self.n_rows += 1

            self.injector.inject(row)

            self.enricher.enrich(row)

            self.validator.validate(row)

            self.merger.merge(row, write)

            entity = self.converter.convert(row)

            self.entity_validator.validate(entity)

            write(entity)

        self.validator.result()
        logger.info(f"Data ({self.n_rows} records) has been imported from {self.source_app}")

    def start_import_process(self):
        try:
            self.row = None

            with ContentsWriter() as writer, \
                    ProgressTicker(f"Import {self.catalogue} {self.entity}", 10000) as progress:

                self.filename = writer.filename

                self.merger.prepare(progress)

                self.import_data(writer.write, progress)

                self.merger.finish(writer.write)

                self.entity_validator.result()

            self.publish()
        except Exception as e:
            # Print error message, the message that caused the error and a short stacktrace
            stacktrace = traceback.format_exc(limit=-5)
            print("Import failed at row {self.n_rows}: {e}", stacktrace)
            # Log the error and a short error description
            logger.error(f'Import failed at row {self.n_rows}: {e}')
            logger.error(
                "Import has failed",
                {
                    "data": {
                        "error": str(e),  # Include a short error description,
                        "row number": self.n_rows,
                        self.source_id: self.row[self.source_id]
                    }
                })
