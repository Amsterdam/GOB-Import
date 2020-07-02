import io
import unittest
from unittest import mock

from gobimport.enricher.bag import BAGEnricher


@mock.patch("gobimport.enricher.bag.logger", mock.MagicMock())
@mock.patch("gobimport.enricher.bag.get_datastore_config", mock.MagicMock())
@mock.patch("gobimport.enricher.bag.DatastoreFactory", mock.MagicMock())
@mock.patch("gobimport.enricher.bag.log_issue", mock.MagicMock())
class TestBAGEnrichment(unittest.TestCase):

    @mock.patch("gobimport.enricher.bag.BAGEnricher._download_amsterdam_sleutel_file", mock.MagicMock())
    @mock.patch("gobimport.enricher.bag.BAGEnricher._get_amsterdamse_sleutel_lookup")
    def test_enrich_nummeraanduidingen(self, mock_lookup):
        mock_lookup.return_value = {
            '1234': {
                'amsterdamse_sleutel': 'sleutel'
            }
        }

        entities = [
            {
                'identificatie': '1234',
                'ligt_in_bag_woonplaats': '1024;3594',
            },
            {
                'identificatie': '4321',
                'ligt_in_bag_woonplaats': '1024',
            }
        ]

        enricher = BAGEnricher("app", "bag", "nummeraanduidingen")
        for entity in entities:
            enricher.enrich(entity)

        # Check if the last value is selected
        self.assertEqual(entities[0]['ligt_in_bag_woonplaats'],'3594')
        self.assertEqual(entities[1]['ligt_in_bag_woonplaats'],'1024')

        # Success path for amsterdamse_sleutel
        self.assertEqual(entities[0]['amsterdamse_sleutel'],'sleutel')

        # Error path for amsterdamse_sleutel, expect an empty attribute
        self.assertEqual(entities[1]['amsterdamse_sleutel'],None)

    @mock.patch("gobimport.enricher.bag.BAGEnricher._download_amsterdam_sleutel_file", mock.MagicMock())
    @mock.patch("gobimport.enricher.bag.BAGEnricher._get_amsterdamse_sleutel_lookup")
    def test_enrich_openbareruimtes(self, mock_lookup):
        mock_lookup.return_value = {
            '1234': {
                'amsterdamse_sleutel': 'sleutel'
            }
        }

        entities = [
            {
                'identificatie': '1234',
                'ligt_in_bag_woonplaats': '1024;3594',
            },
            {
                'identificatie': '4321',
                'ligt_in_bag_woonplaats': '1024;3594',
            },
        ]

        enricher = BAGEnricher("app", "bag", "openbareruimtes")
        for entity in entities:
            enricher.enrich(entity)

        # Check if the both sleutel and straatcode have been added
        self.assertEqual(entities[0]['amsterdamse_sleutel'],'sleutel')

        # Expect values to be empty if no sleutel or straatcode was found
        self.assertEqual(entities[1]['amsterdamse_sleutel'],None)

    @mock.patch("gobimport.enricher.bag.BAGEnricher._download_amsterdam_sleutel_file", mock.MagicMock())
    def test_enrich_verblijfsobjecten(self):
        entities = [
            {
                'identificatie': '1234',
                'gebruiksdoel': '01|doel1;02|doel2',
                'toegang': '01|doel1',
                'pandidentificatie': "1234;5678",
                'fng_code': 500
            },
            {
                'identificatie': '1234',
                'gebruiksdoel': '01|doel1;02|doel2',
                'toegang': '01|doel1',
                'pandidentificatie': "1234;5678",
                'fng_code': 1000
            }
        ]
        enricher = BAGEnricher("app", "bag", "verblijfsobjecten")
        for entity in entities:
            enricher.enrich(entity)

        self.assertEqual(entities[0]['gebruiksdoel'],[{'code': '01', 'omschrijving': 'doel1'}, {'code': '02', 'omschrijving': 'doel2'}])
        self.assertEqual(entities[0]['toegang'],[{'code': '01', 'omschrijving': 'doel1'}])
        self.assertEqual(entities[0]['pandidentificatie'],['1234', '5678'])
        self.assertEqual(entities[0]['fng_omschrijving'],'Ongesubsidieerde bouw (500)')

        # Expect no fng_omschrijving with an invalid code
        self.assertEqual(entities[1]['fng_omschrijving'], None)

        # Expect an empty amsterdamse_sleutel
        self.assertEqual(entities[0]['amsterdamse_sleutel'], None)

    @mock.patch("gobimport.enricher.bag.BAGEnricher._download_amsterdam_sleutel_file", mock.MagicMock())
    def test_enriches(self):
        # Expect a valid BAG collection to be enriched
        self.assertEqual(BAGEnricher.enriches("test", "bag", "woonplaatsen"), True)

        # Expect a invalid BAG collection to not be enriched
        self.assertEqual(BAGEnricher.enriches("test", "bag", "testcollection"), False)

    @mock.patch("gobimport.enricher.bag.BAGEnricher._download_amsterdam_sleutel_file", mock.MagicMock())
    @mock.patch("gobimport.enricher.bag.os.makedirs", mock.MagicMock())
    @mock.patch("gobimport.enricher.bag.tempfile.gettempdir")
    def test_get_filename(self, mock_tempfile):
        mock_tempfile.return_value = "/temp/"
        enricher = BAGEnricher("app", "bag", "enricher")
        file_name = enricher._get_filename("test")

        self.assertEqual(file_name, "/temp/test")

    @mock.patch("gobimport.enricher.bag.BAGEnricher._download_amsterdam_sleutel_file", mock.MagicMock())
    @mock.patch("gobimport.enricher.bag.os.remove")
    def test_cleanup(self, mock_remove):
        enricher = BAGEnricher("app", "bag", "woonplaatsen", download_files=False)
        enricher.amsterdamse_sleutel_file = "test"
        enricher.cleanup()

        mock_remove.assert_called_with("test")

    @mock.patch("gobimport.enricher.bag.BAGEnricher._download_amsterdam_sleutel_file", mock.MagicMock())
    @mock.patch("gobimport.enricher.bag.os.remove")
    def test_cleanup_not_called(self, mock_remove):
        # Remove should not be called for enities without Amsterdamse Sleutel
        enricher = BAGEnricher("app", "bag", "invalid", download_files=False)
        enricher.cleanup()

        mock_remove.assert_not_called()

    @mock.patch("gobimport.enricher.bag.BAGEnricher._get_filename")
    @mock.patch("gobimport.enricher.bag.get_full_container_list")
    @mock.patch("gobimport.enricher.bag.get_object")
    def test_download_amsterdamse_sleutel_file(self, mock_get_object, mock_container_list, mock_filename):
        mock_get_object.return_value = "data"
        mock_container_list.return_value = [
            {'name': 'bag/diva_amsterdamse_sleutel/WPL_20190101.dat'}
        ]

        mock_filename.return_value = "testfile"

        enricher = BAGEnricher("app", "bag", "woonplaatsen", download_files=False)

        mock_open = mock.mock_open()
        with mock.patch('gobimport.enricher.bag.open', mock_open):
            file_name = enricher._download_amsterdam_sleutel_file("bag", "woonplaatsen")

        mock_get_object.assert_called()
        mock_open.assert_called_with("testfile", "wb")

        file_handle = mock_open.return_value.__enter__.return_value
        file_handle.write.assert_called_with("data")

    def test_get_amsterdamse_sleutel_lookup(self):
        enricher = BAGEnricher("app", "bag", "openbareruimtes", download_files=False)
        enricher.amsterdamse_sleutel_file = 'test'

        mock_read_data = 'amsterdamse_sleutel;identificatie;2;3\namsterdamse_sleutel;identificatie;2;3'

        with mock.patch('builtins.open') as mock_open:
            mock_open.return_value.__enter__.return_value = io.StringIO(mock_read_data)
            result = enricher._get_amsterdamse_sleutel_lookup("bag", "openbareruimtes")

        mock_open.assert_called_with("test")

        expected = {
            'identificatie': {'amsterdamse_sleutel': 'amsterdamse_sleutel'}
        }

        self.assertEqual(expected, result)
