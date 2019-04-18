from gobimport.mapping import get_mapping


class Merger:

    def __init__(self, import_client):
        self.import_client = import_client
        self.merge_def = None
        self.merge_items = {}

    def _collect_entity(self, entity, merge_def):
        on = entity[merge_def["on"]]
        self.merge_items[on] = self.merge_items.get(on, {"entities": []})
        self.merge_items[on]["entities"].append(entity)

    def _merge_diva_into_dgdialog(self, entity, write, entities):
        copy = self.merge_def["copy"]
        entities.sort(key=lambda e: int(e["volgnummer"]))

        for diva_entity in entities[:-1]:
            write(diva_entity)

        merge_entity = entities[-1]
        for key in copy:
            entity[key] = merge_entity[key]

    def prepare(self, progress):
        merge_def = self.import_client.source.get("merge")
        if merge_def:
            # Save original dataset
            primary_dataset = self.import_client.dataset.copy()

            # Import merge data
            mapping = get_mapping(merge_def["dataset"])
            self.import_client.init_dataset(mapping)
            self.import_client.import_data(lambda e: self._collect_entity(e, merge_def), progress)

            # Restore original dataset
            self.import_client.init_dataset(primary_dataset)

            id = merge_def["id"]
            self.merge_func = getattr(self, f"_merge_{id}")

            self.merge_def = merge_def

    def merge(self, entity, write):
        if self.merge_def:
            on = self.merge_def["on"]
            merge_item = self.merge_items.get(entity[on])
            if merge_item:
                entities = merge_item["entities"]

                self.merge_func(entity, write, entities)

                del self.merge_items[entity[on]]

    def finish(self, write):
        if self.merge_def:
            for merge_item in self.merge_items.values():
                for entity in merge_item["entities"]:
                    write(entity)
