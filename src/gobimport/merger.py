"""Merger.

Merges a dataset with another dataset.

The majority of the code is data driven.
Specific merging functionality is driven by the name of the merging method.

No default merging method exist.
A merging definition always consists of configuration in model.json and code in the Merger class.

The only merging logic that is implemented is to merge DIVA into DGDialog ("diva_into_dgdialog").

Note: The data to be merged is kept in memory during the import.
When large data collections need to be merged then GOB-Prepare is considered a better place.
Data can then be merged using a database.
"""


from typing import Any

from gobconfig.import_.import_config import get_import_definition_by_filename
from gobcore.model import FIELD
from gobcore.utils import ProgressTicker


class Merger:
    """Merge a dataset with another dataset."""

    def __init__(self, import_client) -> None:
        """Initialise a Merger by providing it with the ImportClient instance.

        The ImportClient instance is used to read the data to be merged.
        :param import_client:
        """
        self.import_client = import_client
        self.merge_def: dict[str, str] = {}
        self.merge_items: dict[str, Any] = {}
        self.merged: set[str] = set()

    def _collect_entity(self, entity: dict[str, Any], merge_def: dict[str, str]) -> None:
        """Collect the data to be merged into a local object.

        :param entity:
        :param merge_def:
        :return:
        """
        on = entity[merge_def["on"]]
        self.merge_items[on] = self.merge_items.get(on, {"entities": []})
        self.merge_items[on]["entities"].append(entity)

    def _merge_diva_into_dgdialog(self, entity: dict[str, Any], write, entities) -> None:
        """DIVA entities are merged into DGDialog.

        By matching volgnummer 1 in DGDialog with the highest volgnummer in DIVA.

        :param entity:
        :param write:
        :param entities:
        :return:
        """
        copy = self.merge_def["copy"]
        entities.sort(key=lambda e: e["volgnummer"])

        # The attributes to copy are derived from the most recent entity
        merge_entity = entities[-1]

        if entity["volgnummer"] == 1:
            # Write the previous entities before the first new entity
            # This will skip merge_entity defined above
            for diva_entity in entities[:-1]:
                write(diva_entity)

        # Copy the specified attributes
        for key in copy:
            entity[key] = merge_entity[key]

        # Update the volgnummer
        entity["volgnummer"] = merge_entity["volgnummer"] + entity["volgnummer"] - 1

    def prepare(self, progress: ProgressTicker) -> None:
        """Prepare the merge process by collecting the data to be merged in a local object (merge_items).

        The import client is used to read the data so that data gets validated and converted.
        The merge function is set to the id of the merge definition.

        :param progress:
        :return:
        """
        merge_def = self.import_client.source.get("merge")
        if merge_def:
            # Save original dataset
            primary_dataset = self.import_client.dataset.copy()

            # Import merge data
            mapping = get_import_definition_by_filename(merge_def["dataset"])
            self.import_client.init_dataset(mapping)
            self.import_client.import_rows(lambda e: self._collect_entity(e, merge_def), progress)

            # Restore original dataset
            self.import_client.init_dataset(primary_dataset)

            id = merge_def["id"]
            self.merge_func = getattr(self, f"_merge_{id}")

            self.merge_def = merge_def

    def is_merged(self, entity: dict[str, Any]) -> bool:
        """Return whether an entity is a 'merged' entity.

        This is the case if True:
         - key is added to self.merged
         - the last volgnummer from merge_entities is equal to `entity` volgnummer
        """
        if not self.merge_def:
            return False

        on = self.merge_def["on"]
        key = entity[on]
        return (
            key in self.merged
            and key in self.merge_items
            and self.merge_items[key]["entities"]  # Merger.prepare: not all merge_items are populated yet
            and self.merge_items[key]["entities"][-1][FIELD.SEQNR] == entity[FIELD.SEQNR]
        )

    def merge(self, entity: dict[str, Any], write) -> None:
        """Merge entity if dataset merge definition exists.

        If a merge definition exists for the current dataset, the entity is merged
        with the entities in self.merge_items.

        :param entity:
        :param write:
        :return:
        """
        if self.merge_def:
            on = self.merge_def["on"]

            if merge_item := self.merge_items.get(entity[on]):
                self.merge_func(entity, write, merge_item["entities"])
                self.merged.add(entity[on])

    def finish(self, write) -> None:
        """Apply write (Callable[[entity], None]) on remaining entities.

        During the merging entities get written.
        Any entities that didn't appear in the merge process get written at the end of the import
        by calling this method

        :param write:
        :return:
        """
        if self.merge_def:
            for on, merge_item in self.merge_items.items():
                if on not in self.merged:
                    for entity in merge_item["entities"]:
                        write(entity)
            self.merge_items = {}
