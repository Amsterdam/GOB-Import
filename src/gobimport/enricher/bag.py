"""
BAG enrichment

"""
import csv
import os
import re
import tempfile

from objectstore.objectstore import get_full_container_list, get_object

from gobcore.datastore.factory import DatastoreFactory
from gobconfig.datastore.config import get_datastore_config
from gobcore.logging.logger import logger

from gobimport.config import CONTAINER_BASE
from gobimport.enricher.enricher import Enricher
from gobcore.quality.issue import QA_CHECK, QA_LEVEL, Issue, log_issue


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

FINANCIERINGSCODE_MAPPING = {
    '501': 'Eigen bouw (501)',
    '110': 'Sociale huursector woningwet (110)',
    '200': 'Premiehuur Profit (200)',
    '201': 'Premiehuur Profit voor 1985 (201)',
    '220': 'Wet Materiële Oorlogsschade (220)',
    '250': 'Huurwoningen Beleggers (250)',
    '271': 'Premiehuur Profit met gemeentegarantie (271)',
    '274': 'Sociale koop (voorheen Premiekoop A) (274)',
    '301': 'Premiekoop voor 1985 (301)',
    '373': 'Subsidie Premiekoop A (373)',
    '374': 'Subsidie Premiekoop B (374)',
    '375': 'Premiewoningen (375)',
    '466': 'V.S.E.B. (466)',
    '475': 'V.S.E.B. Premiekoop C (475)',
    '476': 'V.S.E.B. Premiehuur C (476)',
    '500': 'Ongesubsidieerde bouw (500)',
    '999': 'DEFAULT VOOR CONVERSIE',
    '477': 'Middeldure huur (477)',
    '478': 'Middeldure koop (478)',
}


class BAGEnricher(Enricher):

    @classmethod
    def enriches(cls, app_name, catalog_name, entity_name):
        if catalog_name == "bag":
            enricher = BAGEnricher(app_name, catalog_name, entity_name, download_files=False)
            return enricher._enrich_entity is not None or entity_name in DIVA_ABBREVIATIONS

    def __init__(self, app_name, catalogue_name, entity_name, download_files=True):
        self.multiple_values_logged = False
        self.entity_name = entity_name

        if download_files and self.entity_name in DIVA_ABBREVIATIONS:
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
        datastore = DatastoreFactory.get_datastore(get_datastore_config('Basisinformatie'))
        datastore.connect()
        connection = datastore.connection

        file_name = self._get_filename(entity_name)

        # Use the container base env variable
        files = get_full_container_list(connection, CONTAINER_BASE)

        # Filter for the .dat files, and get the abbreviation and file date
        pattern = re.compile(f"{DIVA_FILE_PATH}(.+)_([0-9]+)\.dat$")

        for file in files:
            match_result = pattern.search(file["name"])
            if match_result:
                # Find the matching DIVA abbreviation for the entity name
                if DIVA_ABBREVIATIONS[entity_name] == match_result.group(1):
                    # Store the file in temporary storage
                    obj = get_object(connection, file, CONTAINER_BASE)
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

        return results

    def cleanup(self):
        """
        Cleanup an enricher e.g. delete temporary files

        :return:
        """
        # Remove the temporary file for amsterdamseSleutel
        if self.entity_name in DIVA_ABBREVIATIONS:
            os.remove(self.amsterdamse_sleutel_file)

    def enrich(self, entity):
        """
        Enrich a single entity, override with default enrichment for all BAG collections

        :param entity:
        :return:
        """
        # Add DIVA amsterdamseSleutel enrichment for all entities in DIVA_ABBREVIATIONS
        if self.entity_name in DIVA_ABBREVIATIONS:
            # Create an empty dict to make sure attributes exist on the entity
            empty_dict = {
                'amsterdamse_sleutel': None,
            }

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
                log_issue(logger, QA_LEVEL.WARNING,
                          Issue(QA_CHECK.Value_1_1_reference, nummeraanduiding, None, 'ligt_in_bag_woonplaats'))
                self.multiple_values_logged = True

    def enrich_dossier(self, dossier):
        dossier['heeft_bag_brondocument'] = dossier['heeft_bag_brondocument'].split(";")

    def enrich_verblijfsobject(self, verblijfsobject):

        # Get the omschrijving for the finiancieringscode
        verblijfsobject['fng_omschrijving'] = FINANCIERINGSCODE_MAPPING.get(str(verblijfsobject['fng_code']))

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
