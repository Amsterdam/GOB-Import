"""
Gebieden enrichment

"""
from datetime import datetime
import requests

from gobcore.logging.logger import logger
from gobimport.enricher.enricher import Enricher

from shapely.geometry import shape
from shapely.wkt import loads


CBS_BUURTEN_API = 'https://geodata.nationaalgeoregister.nl/wijkenbuurten2018/wfs' \
                 '?SERVICE=WFS&VERSION=1.0.0&REQUEST=GetFeature&TYPENAME=wijkenbuurten2018:cbs_buurten_2018' \
                 '&SRSNAME=EPSG:28992&outputFormat=application/json' \
                 '&FILTER=<ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">' \
                 '<ogc:PropertyIsEqualTo><ogc:PropertyName>gemeentenaam</ogc:PropertyName>'\
                 '<ogc:Literal>Amsterdam</ogc:Literal></ogc:PropertyIsEqualTo></ogc:Filter>'


CBS_WIJKEN_API = 'https://geodata.nationaalgeoregister.nl/wijkenbuurten2018/wfs' \
                 '?SERVICE=WFS&VERSION=1.0.0&REQUEST=GetFeature&TYPENAME=wijkenbuurten2018:cbs_wijken_2018' \
                 '&SRSNAME=EPSG:28992&outputFormat=application/json' \
                 '&FILTER=<ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">' \
                 '<ogc:PropertyIsEqualTo><ogc:PropertyName>gemeentenaam</ogc:PropertyName>'\
                 '<ogc:Literal>Amsterdam</ogc:Literal></ogc:PropertyIsEqualTo></ogc:Filter>'


class GebiedenEnricher(Enricher):

    @classmethod
    def enriches(cls, app_name, catalog_name, entity_name):
        if catalog_name == "gebieden":
            enricher = GebiedenEnricher(app_name, catalog_name, entity_name)
            return enricher._enrich_entity is not None

    def __init__(self, app_name, catalogue_name, entity_name):
        super().__init__(app_name, catalogue_name, entity_name, methods={
            "buurten": self.enrich_buurt,
            "wijken": self.enrich_wijk,
            "ggwgebieden": self.enrich_ggwgebied,
            "ggpgebieden": self.enrich_ggpgebied,
        })

        self.features = {}

    def enrich_buurt(self, buurt):
        self._add_cbs_code(buurt, CBS_BUURTEN_API, 'buurt')

    def enrich_wijk(self, wijk):
        self._add_cbs_code(wijk, CBS_WIJKEN_API, 'wijk')

    def enrich_ggwgebied(self, ggwgebied):
        self._enrich_ggw_ggp_gebied(ggwgebied, "GGW")

    def enrich_ggpgebied(self, ggpgebied):
        self._enrich_ggw_ggp_gebied(ggpgebied, "GGP")

    def _add_cbs_code(self, entity, url, type):
        """
        Gets the CBS codes and tries to match them based on the inside
        point of the geometry. Returns the entities enriched with CBS Code.

        :param entities: a list of entities ready for enrichment
        :param url: the url to the cbs code API
        :param type: the type of entity, needed to get the correct values from the API
        :return: the entities enriched with CBS Code
        """
        if not self.features.get(type):
            self.features[type] = _get_cbs_features(url, type)

        # Leave entities without datum_einde_geldigheid empty
        if entity['eind_geldigheid']:
            entity['cbs_code'] = ''
            return

        # Check which CBS feature lays within the geometry
        match = _match_cbs_features(entity, self.features[type])

        entity['cbs_code'] = match['code'] if match else ''

        # Show a warning if the names do not match with CBS
        if match and entity['naam'] != match['naam']:
            msg = "Naam and CBS naam don't match"
            extra_data = {
                'id': msg,
                'data': {
                    'application': self.app_name,
                    'identificatie': entity['identificatie'],
                    'naam': entity['naam'],
                    'cbs_naam': match['naam'],
                }
            }
            logger.warning(msg, extra_data)

    def _enrich_ggw_ggp_gebied(self, entity, prefix):
        """Enrich GGW or GGP Gebieden

        Add:
        - Missing identificatie
        - Set registratiedatum to the date of the file
        - Set volgnummer to a number that corresponds to the registratiedatum
        - Interpret BUURTEN as a comma separated list of codes
        - Convert Excel DateTime values to date strings in YYYY-MM-DD format

        :param entities: a list of entities
        :param prefix: GGW or GGP
        :return: None
        """
        entity["BUURTEN"] = entity["BUURTEN"].split(", ")
        entity["_IDENTIFICATIE"] = None
        entity["registratiedatum"] = entity["_file_info"]["last_modified"]
        entity["volgnummer"] = _volgnummer_from_date(entity["registratiedatum"], "%Y-%m-%dT%H:%M:%S.%f")
        for date in [f"{prefix}_{date}" for date in ["BEGINDATUM", "EINDDATUM", "DOCUMENTDATUM"]]:
            if entity[date] is not None:
                entity[date] = str(entity[date])[:10]   # len "YYYY-MM-DD" = 10


def _volgnummer_from_date(date_str, date_format):
    """Generate a volgnummer from a date

    :param date_str: date string to derive volgnummer from
    :param date_format: format of date string
    :return: a volgnummer that is higher for recent dates and lower for oldest dates
    """
    return int(datetime.strptime(date_str, date_format).timestamp())


def _match_cbs_features(entity, features):
    """
    Match the geometry of an entity to the CBS inside point

    :param entity: the entity to match to
    :param features: the cbs features
    :return: the matched cbs feature or none
    """
    geom = loads(entity['geometrie'])
    match = None

    for feature in features:
        if geom.contains(feature['geometrie']) and not match:
            match = feature
        elif geom.contains(feature['geometrie']) and match:
            msg = "Entity already had a match"
            extra_data = {
                'data': {
                    'id': msg,
                    'naam': entity['naam'],
                    'match': match['naam'],
                    'cbs_naam': feature['naam'],
                }
            }
            logger.warning(msg, extra_data)

    return match


def _get_cbs_features(url, type):
    """
    Gets the CBS codes from the API and returns a list of dicts with the naam,
    code and geometrie of the feature (wijk or buurt)

    :param url: the url to the cbs code API
    :param type: the type of entity, needed to get the correct values from the API
    :return: a list of dicts with CBS Code, CBS naam and geometry
    """
    response = requests.get(url)
    assert response.ok
    cbs_result = response.json()

    features = []
    for feature in cbs_result['features']:
        # Skip features if they exist only of water
        if feature['properties']['water'] == 'JA':
            continue

        # Include the naam, code and representative point of the feature
        cleaned_feature = {
            'geometrie': shape(feature['geometry']).representative_point(),
            'code': feature['properties'][f'{type}code'],
            'naam': feature['properties'][f'{type}naam'],
        }
        features.append(cleaned_feature)
    return features
