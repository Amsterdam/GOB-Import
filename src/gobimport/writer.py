"""
Write GOB entities to file

"""
import json

from gobcore.typesystem.json import GobTypeJSONEncoder
from gobcore.message_broker.offline_contents import _get_filename, _get_unique_name


class Writer:

    def __init__(self):
        """
        Opens a file
        The entities are written to the file as an array
        """
        unique_name = _get_unique_name()
        self.filename = _get_filename(unique_name)
        self.file = open(self.filename, 'w')
        self.file.write("[")
        self.empty = True

    def write(self, entity):
        """
        Write an entity to the file

        Separate entities with a comma
        :param entity:
        :return:
        """
        if not self.empty:
            self.file.write(",\n")
        self.file.write(json.dumps(entity, cls=GobTypeJSONEncoder, allow_nan=False))
        self.empty = False

    def close(self):
        """
        Terminates the array and closes the file
        :return:
        """
        self.file.write("]")
        self.file.close()
