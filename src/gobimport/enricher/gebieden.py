"""
Gebieden enrichment

"""
from gobconfig.datastore.config import get_datastore_config
from gobcore.datastore.objectstore import ObjectDatastore
from gobimport.enricher.enricher import Enricher

CBS_CODES_BUURT = 'gebieden/Buurten/CBScodes_buurt.xlsx'
CBS_CODES_WIJK = 'gebieden/Wijken/CBScodes_wijk.xlsx'


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
        self._add_cbs_code(buurt, CBS_CODES_BUURT, 'buurt')

    def enrich_wijk(self, wijk):
        self._add_cbs_code(wijk, CBS_CODES_WIJK, 'wijk')

    def enrich_ggwgebied(self, ggwgebied):
        self._enrich_ggw_ggp_gebied(ggwgebied, "GGW")

    def enrich_ggpgebied(self, ggpgebied):
        self._enrich_ggw_ggp_gebied(ggpgebied, "GGP")

    def _add_cbs_code(self, entity: dict, path: str, type_: str):
        """
        Gets the CBS codes and tries to match them based on `type_` code.
        Returns the entities enriched with CBS Code.

        :param entity: an entity ready for enrichment
        :param path: the path to source file
        :param type_: the type of entity (wijk or buurt)
        :return: the entity enriched with CBS Code
        """
        if not self.features.get(type_):
            self.features[type_] = _get_cbs_features(path)

        match = self.features[type_].get(entity["code"], {})
        entity["cbs_code"] = match.get("code")

    def _enrich_ggw_ggp_gebied(self, entity: dict, prefix: str):
        """Enrich GGW or GGP Gebieden

        Add:
        - Missing identificatie
        - Set registratiedatum to the date of the file
        - Set volgnummer to a number that corresponds to the registratiedatum
        - Interpret BUURTEN as a comma separated list of codes
        - Convert Excel DateTime values to date strings in YYYY-MM-DD format

        :param entity: a ggw or ggp gebied
        :param prefix: GGW or GGP
        :return: None
        """
        entity["BUURTEN"] = entity["BUURTEN"].split(", ")
        entity["_IDENTIFICATIE"] = None
        entity["registratiedatum"] = entity["_file_info"]["last_modified"]
        for date in [f"{prefix}_{date}" for date in ["BEGINDATUM", "EINDDATUM", "DOCUMENTDATUM"]]:
            if entity[date] is not None:
                entity[date] = str(entity[date])[:10]   # len "YYYY-MM-DD" = 10


def _get_cbs_features(path: str) -> dict[str, dict[str, str]]:
    """
    Gets the CBS codes from the Objectstore and returns a list of dicts with the naam,
    code (wijk or buurt).

    :param path: the path to source file
    :return: a list of dicts with CBS Code and CBS naam, mapped on the local code.
    """
    datastore = ObjectDatastore(
        connection_config=get_datastore_config("Basisinformatie"),
        read_config={"file_filter": path, "file_type": "XLS"}
    )
    datastore.connect()
    features = {row[0]: {"code": row[1], "naam": row[2]} for row in datastore.query('')}
    datastore.disconnect()
    return features
