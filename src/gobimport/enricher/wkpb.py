"""
WKPB enrichment

"""
from gobimport.enricher.enricher import Enricher


class WKPBEnricher(Enricher):

    @classmethod
    def enriches(cls, app_name, catalog_name, entity_name):
        if catalog_name == "wkpb":
            enricher = WKPBEnricher(app_name, catalog_name, entity_name)
            return enricher._enrich_entity is not None

    def __init__(self, app_name, catalogue_name, entity_name):

        self.multiple_values_logged = False

        super().__init__(app_name, catalogue_name, entity_name, methods={
            "beperkingen": self.enrich_beperkingen,
            "dossiers": self.enrich_dossier,
        })

    def enrich_beperkingen(self, beperking):
        if beperking['belast_brk_kadastraal_object']:
            beperking['belast_brk_kadastraal_object'] = beperking['belast_brk_kadastraal_object'].split(";")

    def enrich_dossier(self, dossier):
        dossier['heeft_wkpb_brondocument'] = dossier['heeft_wkpb_brondocument'].split(";")
