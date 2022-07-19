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

from gobcore.enum import ImportMode
from gobcore.message_broker.offline_contents import ContentsWriter
from gobcore.utils import ProgressTicker

from gobimport.converter import Converter
from gobimport.enricher import BaseEnricher
from gobimport.entity_validator import EntityValidator
from gobimport.injections import Injector
from gobimport.merger import Merger
from gobimport.reader import Reader
from gobimport.validator import Validator

from typing import Optional


class ImportClient:
    """Main class for an import client

    This class serves as the main client for which the import can be configured in a dataset.json

    """

    n_rows = 0

    def __init__(self, dataset, msg, logger, mode: ImportMode = ImportMode.FULL):
        self.mode = mode
        self.logger = logger

        self.init_dataset(dataset)

        self.entity_validator = EntityValidator(self.catalogue, self.entity, self.func_source_id)
        self.merger = Merger(self)

        self.header = msg.get('header', {})
        self.logger.info(f"Import dataset {self.entity} from {self.source_app} (mode = {self.mode.name}) started")

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
        self.enricher = BaseEnricher(self.source_app, self.catalogue, self.entity)
        self.validator = Validator(self.source_app, self.catalogue, self.entity, self.dataset)
        self.converter = Converter(self.catalogue, self.entity, self.dataset)

    def get_result_msg(self):
        """The result of the import needs to be published.

        Publication includes a header, summary and results
        The header is for identification purposes
        The summary is for the interpretation of the results. Was the import successful, what er the metrics, etc
        The results is the imported data in GOB format

        :return:
        """
        header = {
            **self.header,
            "depends_on": self.dataset['source'].get('depends_on', {}),
            "enrich": self.dataset['source'].get('enrich', {}),
            "version": self.dataset['version'],
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

        summary = {
            'num_records': self.n_rows
        }

        log_msg = f"Import dataset {self.entity} from {self.source_app} completed. "
        if self.mode == ImportMode.DELETE:
            log_msg += "0 records imported, all known entities will be marked as deleted."
        else:
            log_msg += f"{summary['num_records']} records were read from the source."

        # Log end of import process
        self.logger.info(log_msg, kwargs={"data": summary})

        summary.update(self.logger.get_summary())

        import_message = {
            "header": header,
            "summary": summary,
            "contents_ref": self.filename
        }

        return import_message

    def import_rows(self, write, progress):
        self.logger.info(f"Connect to {self.source_app}")
        reader = Reader(self.source, self.source_app, self.dataset, self.mode)
        reader.connect()

        self.logger.info(f"Start import from {self.source_app}")
        self.n_rows = 0
        for row in reader.read():
            progress.tick()

            self.row = row
            self.n_rows += 1

            self.injector.inject(row)

            self.enricher.enrich(row)

            self.merger.merge(row, write)

            entity = self.converter.convert(row)

            # validator and entity_validator build up sets of primary keys from the dataset
            # -> higher memory consumption
            self.validator.validate(entity)
            self.entity_validator.validate(entity)

            write(entity)

        self.validator.result()

        self.logger.info(f"{self.n_rows} records have been imported from {self.source_app}")

        min_rows = self.dataset.get("min_rows", 1)
        if self.mode == ImportMode.FULL and self.n_rows < min_rows:
            # Default requirement for full imports is a non-empty dataset
            self.logger.error(f"Too few records imported: {self.n_rows} < {min_rows}")

    def import_dataset(self, destination: Optional[str] = None):
        try:
            self.row = None

            with ContentsWriter(destination) as writer, \
                    ProgressTicker(f"Import {self.catalogue} {self.entity}", 10000) as progress:

                self.filename = writer.filename

                # DELETE: Skip import rows -> write empty file
                # mark all entities as deleted
                if self.mode != ImportMode.DELETE:
                    self.merger.prepare(progress)
                    self.import_rows(writer.write, progress)
                    self.merger.finish(writer.write)
                    self.entity_validator.result()

        except Exception as e:
            # Print error message, the message that caused the error and a short stacktrace
            stacktrace = traceback.format_exc(limit=-5)
            print(f"Import failed at row {self.n_rows}: {e}", stacktrace)
            # Log the error and a short error description
            self.logger.error(f'Import failed at row {self.n_rows}: {e}')
            self.logger.error(
                "Import has failed",
                {
                    "data": {
                        "error": str(e),  # Include a short error description,
                        "row number": self.n_rows,
                        self.source_id: "" if self.row is None else self.row[self.source_id],
                    }
                })

        return self.get_result_msg()
