"""
BAG enrichment

"""
import csv
import os
import re
import tempfile

from objectstore.objectstore import get_full_container_list, get_object

from gobcore.database.connector import connect_to_objectstore
from gobcore.logging.logger import logger

from gobimport.config import get_objectstore_config
from gobimport.enricher.enricher import Enricher


CODE_TABLE_FIELDS = ['code', 'omschrijving']

DIVA_ABBREVIATIONS = {
    'woonplaatsen': 'WPL',
    'verblijfsobjecten': 'VBO',
    'standplaatsen': 'STA',
    'panden': 'PND',
    'openbareruimtes': 'OPR',
    'nummeraanduidingen': 'NUM',
    'ligplaatsen': 'LIG',
}
DIVA_FILE_PATH = "bag/diva_amsterdamse_sleutel/"


class BAGEnricher(Enricher):

    @classmethod
    def enriches(cls, app_name, catalog_name, entity_name):
        if catalog_name == "bag":
            enricher = BAGEnricher(app_name, catalog_name, entity_name, download_files=False)
            return enricher._enrich_entity is not None or entity_name in DIVA_ABBREVIATIONS

    def __init__(self, app_name, catalogue_name, entity_name, download_files=True):
        self.multiple_values_logged = False
        self.entity_name = entity_name

        if download_files:
            # Download the latest file with the Amsterdamse sleutel and create a lookup dict
            self.amsterdamse_sleutel_file = self._download_amsterdam_sleutel_file(catalogue_name, entity_name)
            self.amsterdamse_sleutel_lookup = self._get_amsterdamse_sleutel_lookup(catalogue_name, entity_name)

        super().__init__(app_name, catalogue_name, entity_name, methods={
            "nummeraanduidingen": self.enrich_nummeraanduiding,
            "verblijfsobjecten": self.enrich_verblijfsobject,
            "dossiers": self.enrich_dossier,
            "ligplaatsen": self.extract_nevenadressen,
            "standplaatsen": self.extract_nevenadressen,
        })

    def _get_filename(self, name):
        """Gets the full filename given a the name of a file

        :param name:
        :return:
        """
        dir = tempfile.gettempdir()
        # Create the path if the path not yet exists
        temp_filename = os.path.join(dir, name)
        os.makedirs(os.path.dirname(temp_filename), exist_ok=True)
        return temp_filename

    def _download_amsterdam_sleutel_file(self, catalogue_name, entity_name):
        """Download the Amsterdamse sleutel file from the objectstore

        :param catalogue_name:
        :param entity_name:
        :return:
        """
        connection, user = connect_to_objectstore(get_objectstore_config('Basisinformatie'))
        container_name = os.getenv("CONTAINER_BASE", "acceptatie")

        file_name = self._get_filename(entity_name)

        # Use the container base env variable
        files = get_full_container_list(connection, container_name)

        # Filter for the .dat files, and get the abbreviation and file date
        pattern = re.compile(f"{DIVA_FILE_PATH}(.+)_([0-9]+)\.dat$")

        for file in files:
            match_result = pattern.search(file["name"])
            if match_result:
                # Find the matching DIVA abbreviation for the entity name
                if DIVA_ABBREVIATIONS[entity_name] == match_result.group(1):
                    # Store the file in temporary storage
                    obj = get_object(connection, file, container_name)
                    with open(file_name, 'wb') as output:
                        output.write(obj)
        return file_name

    def _get_amsterdamse_sleutel_lookup(self, catalogue_name, entity_name):
        """
        Get a lookup table for the Amsterdame Sleutel for this entity

        :param catalogue_name:
        :param entity_name:
        :return:
        """
        results = {}

        with open(self.amsterdamse_sleutel_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')

            for row in csv_reader:
                # Match on the second column in the .dat file and get the Amsterdamse Sleutel from column 2
                results[row[1]] = {
                    'amsterdamse_sleutel': row[0]
                }

                # For openbare ruimtes get the straatcode as well
                if entity_name == 'openbareruimtes':
                    results[row[1]]['straatcode'] = row[4]

        return results

    def cleanup(self):
        """
        Cleanup an enricher e.g. delete temporary files

        :return:
        """
        # Remove the temporary file
        os.remove(self.amsterdamse_sleutel_file)

    def enrich(self, entity):
        """
        Enrich a single entity, override with default enrichment for all BAG collections

        :param entity:
        :return:
        """
        # Create an empty dict to make sure attributes exist on the entity
        empty_dict = {
            'amsterdamse_sleutel': ''
        }
        if self.entity_name == 'openbareruimtes':
            empty_dict['straatcode'] = ''

        # Add Amsterdam Sleutel for all entities
        amsterdamse_sleutel = self.amsterdamse_sleutel_lookup.get(entity['identificatie'], empty_dict)
        entity.update(amsterdamse_sleutel)

        if self._enrich_entity:
            self._enrich_entity(entity)

    def enrich_nummeraanduiding(self, nummeraanduiding):

        # ligt_in_woonplaats can have multiple values, use the last value and log a warning
        bronwaarde = nummeraanduiding['ligt_in_bag_woonplaats']
        if bronwaarde and ';' in bronwaarde:
            nummeraanduiding['ligt_in_bag_woonplaats'] = bronwaarde.split(';')[-1]

            if not self.multiple_values_logged:
                msg = f"multiple values for a single reference found"
                extra_data = {
                    'id': msg,
                    'data': {
                        'bronwaarde': bronwaarde,
                        'attribute': 'ligt_in_woonplaats'
                    }
                }
                logger.warning(msg, extra_data)
                self.multiple_values_logged = True

    def enrich_dossier(self, dossier):
        dossier['heeft_bag_brondocument'] = dossier['heeft_bag_brondocument'].split(";")

    def enrich_verblijfsobject(self, verblijfsobject):

        self.extract_nevenadressen(verblijfsobject)

        gebruiksdoelen = verblijfsobject['gebruiksdoel'].split(";")
        verblijfsobject['gebruiksdoel'] = []
        for gebruiksdoel in gebruiksdoelen:
            verblijfsobject['gebruiksdoel'].append(_extract_code_table(gebruiksdoel, CODE_TABLE_FIELDS))

        # Extract code tables for fields
        _extract_code_tables(verblijfsobject, ['gebruiksdoel_woonfunctie', 'gebruiksdoel_gezondheidszorg'])

        # Toegang can be a multivalue code table
        if verblijfsobject['toegang']:
            toegangen = verblijfsobject['toegang'].split(";")
            verblijfsobject['toegang'] = []
            for toegang in toegangen:
                verblijfsobject['toegang'].append(_extract_code_table(toegang, CODE_TABLE_FIELDS))

        if verblijfsobject['pandidentificatie']:
            verblijfsobject['pandidentificatie'] = verblijfsobject['pandidentificatie'].split(";")

    def extract_nevenadressen(self, entity):
        """Extract multiple nevenadressen

        :param entity: an imported entity
        :return:
        """
        if entity['nummeraanduidingid_neven']:
            entity['nummeraanduidingid_neven'] = entity['nummeraanduidingid_neven'].split(";")


def _extract_code_tables(entity, fields):
    """Extract code tables for a list of field

    Code tables are build using CODE_TABLE_FIELDS and updated on the entity

    :param entity: an imported entity
    :return:
    """
    for field in fields:
        if entity[field]:
            entity[field] = _extract_code_table(entity[field], CODE_TABLE_FIELDS)


def _extract_code_table(value, fields, separator="|"):
    """Extract code table for a list of fields

    Splits a value on the separator and returns the values mapped on a list of fields

    :param value: a value to extract into a code table
    :param fields: a list of field to map onto
    :param separator: the value to separate the string on
    :return:
    """
    code_table = {}
    values = value.split(separator)
    for count, value in enumerate(values):
        code_table[fields[count]] = value
    return code_table
