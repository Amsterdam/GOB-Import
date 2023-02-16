"""Import Enricher."""


from abc import ABC, abstractmethod
from typing import Any, Callable


class Enricher(ABC):
    """Abstract base class for any enrichment class."""

    @classmethod
    @abstractmethod
    def enriches(cls, app_name: str, catalog_name: str, entity_name: str) -> bool:
        """Tell wether this class enriches the given catalog - entity.

        :param catalog_name:
        :param entity_name:
        :return:
        """
        pass  # pragma: no cover

    def __init__(
        self, app_name: str, catalogue_name: str, entity_name: str, methods: dict[str, Callable[[dict[str, str]], Any]]
    ) -> None:
        """Initialise Enricher.

        Given the methods and entity name, the enrich_entity method is set.

        :param methods:
        :param entity_name:
        """
        self.app_name = app_name
        self.catalogue_name = catalogue_name
        self.entity_name = entity_name
        self._enrich_entity = methods.get(entity_name)

    def enrich(self, entity: dict[str, Any]) -> None:
        """Enrich a single entity.

        :param entity:
        :return:
        """
        if self._enrich_entity:
            self._enrich_entity(entity)
