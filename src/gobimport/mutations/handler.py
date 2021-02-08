from gobcore.exceptions import GOBException

from gobimport.database.model import MutationImport
from gobimport.mutations.bagextract import BagExtractMutationsHandler


class MutationsHandler:
    HANDLERS = {
        "BAGExtract": BagExtractMutationsHandler,
    }

    def __init__(self, dataset: dict):
        self.dataset = dataset
        self.application = self.get_application(dataset)

        if self.application not in self.HANDLERS:
            raise GOBException(f"No handler defined for {self.application}")
        self.handler = self.HANDLERS[self.application]()

    @staticmethod
    def get_application(dataset: dict):
        return dataset.get('source', {}).get('application')

    @classmethod
    def is_mutations_import(cls, dataset: dict):
        application = cls.get_application(dataset)
        return application in cls.HANDLERS.keys()

    def get_next_import(self, last_import: MutationImport) -> tuple[MutationImport, dict]:
        return self.handler.handle_import(last_import, self.dataset)

    def have_next(self, mutation_import: MutationImport):
        return self.handler.have_next(mutation_import, self.dataset)
