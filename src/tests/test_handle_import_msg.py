from typing import Generator
from unittest import mock
from unittest.mock import MagicMock

import pytest

from gobimport.__main__ import handle_import_msg


class TestServiceHandler:

    @pytest.fixture
    def gob_logger_manager_mock(self) -> Generator[MagicMock, None, None]:
        # FIXME: share in gobcore
        with mock.patch("gobcore.logging.logger.LoggerManager") as p:
            yield p

    def test_handle_import_msg(self, gob_logger_manager_mock, database):
        msg = {
            "header": {
                "catalogue": "bag",
                "collection": "ligplaatsen",
                "application": "BAGExtract"
            }
        }
        # This imports the data from bag_ligplaatsen.
        # To make this work, fixtures with data should be loaded from bag_ligplaatsen.

        # Data from accept
        """
        8,0457,0457020000002626.1,2022-01-15,"{""heeftAlsHoofdadres/NummeraanduidingRef"": ""0457200000202010"", ""voorkomen/Voorkomen/voorkomenidentificatie"": ""1"", ""voorkomen/Voorkomen/beginGeldigheid"": ""2010-07-29"", ""voorkomen/Voorkomen/tijdstipRegistratie"": ""2010-11-26T08:51:38.000"", ""voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV"": ""2010-11-26T09:01:35.742"", ""identificatie"": ""0457020000002626"", ""status"": ""Plaats aangewezen"", ""geometrie"": ""POLYGON ((133187.488 478733.361,133188.643 478725.651,133206.945 478728.392,133205.79 478736.102,133187.488 478733.361))"", ""geconstateerd"": ""N"", ""documentdatum"": ""2010-07-20"", ""documentnummer"": ""Z.10-8226/D.10-5338""}"
        9,0457,0457020000002627.1,2022-01-15,"{""heeftAlsHoofdadres/NummeraanduidingRef"": ""0457200000202011"", ""voorkomen/Voorkomen/voorkomenidentificatie"": ""1"", ""voorkomen/Voorkomen/beginGeldigheid"": ""2010-07-29"", ""voorkomen/Voorkomen/tijdstipRegistratie"": ""2010-11-26T08:51:38.000"", ""voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV"": ""2010-11-26T09:01:35.747"", ""identificatie"": ""0457020000002627"", ""status"": ""Plaats aangewezen"", ""geometrie"": ""POLYGON ((133236.512 478737.914,133216.351 478736.914,133216.718 478729.496,133236.879 478730.495,133236.512 478737.914))"", ""geconstateerd"": ""N"", ""documentdatum"": ""2010-07-20"", ""documentnummer"": ""Z.10-8226/D.10-5338""}"
        10,0457,0457020000002628.1,2022-01-15,"{""heeftAlsHoofdadres/NummeraanduidingRef"": ""0457200000202012"", ""voorkomen/Voorkomen/voorkomenidentificatie"": ""1"", ""voorkomen/Voorkomen/beginGeldigheid"": ""2010-07-29"", ""voorkomen/Voorkomen/tijdstipRegistratie"": ""2010-11-26T08:51:38.000"", ""voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV"": ""2010-11-26T09:01:35.751"", ""identificatie"": ""0457020000002628"", ""status"": ""Plaats aangewezen"", ""geometrie"": ""POLYGON ((133260.546 478739.095,133248.627 478737.701,133249.322 478731.752,133261.241 478733.145,133260.546 478739.095))"", ""geconstateerd"": ""N"", ""documentdatum"": ""2010-07-20"", ""documentnummer"": ""Z.10-8226/D.10-5338""}"
        11,0457,0457020000002629.1,2022-01-15,"{""heeftAlsHoofdadres/NummeraanduidingRef"": ""0457200000202013"", ""voorkomen/Voorkomen/voorkomenidentificatie"": ""1"", ""voorkomen/Voorkomen/beginGeldigheid"": ""2010-07-29"", ""voorkomen/Voorkomen/tijdstipRegistratie"": ""2010-11-26T08:51:38.000"", ""voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV"": ""2010-11-26T09:01:35.757"", ""identificatie"": ""0457020000002629"", ""status"": ""Plaats aangewezen"", ""geometrie"": ""POLYGON ((133275.545 478741.791,133263.758 478740.309,133264.686 478732.927,133276.473 478734.409,133275.545 478741.791))"", ""geconstateerd"": ""N"", ""documentdatum"": ""2010-07-20"", ""documentnummer"": ""Z.10-8226/D.10-5338""}"
        12,0457,0457020000002630.1,2022-01-15,"{""heeftAlsHoofdadres/NummeraanduidingRef"": ""0457200000202014"", ""voorkomen/Voorkomen/voorkomenidentificatie"": ""1"", ""voorkomen/Voorkomen/beginGeldigheid"": ""2010-07-29"", ""voorkomen/Voorkomen/tijdstipRegistratie"": ""2010-11-26T08:51:38.000"", ""voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV"": ""2010-11-26T09:01:35.761"", ""identificatie"": ""0457020000002630"", ""status"": ""Plaats aangewezen"", ""geometrie"": ""POLYGON ((133298.071 478742.421,133282.127 478741.083,133282.712 478734.108,133298.656 478735.446,133298.071 478742.421))"", ""geconstateerd"": ""N"", ""documentdatum"": ""2010-07-20"", ""documentnummer"": ""Z.10-8226/D.10-5338""}"
        13,0457,0457020000002631.1,2022-01-15,"{""heeftAlsHoofdadres/NummeraanduidingRef"": ""0457200000202015"", ""voorkomen/Voorkomen/voorkomenidentificatie"": ""1"", ""voorkomen/Voorkomen/beginGeldigheid"": ""2010-07-29"", ""voorkomen/Voorkomen/tijdstipRegistratie"": ""2010-11-26T08:51:39.000"", ""voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV"": ""2010-11-26T09:01:35.767"", ""identificatie"": ""0457020000002631"", ""status"": ""Plaats aangewezen"", ""geometrie"": ""POLYGON ((133319.602 478742.103,133306.117 478741.118,133306.563 478734.918,133320.094 478735.784,133319.602 478742.103))"", ""geconstateerd"": ""N"", ""documentdatum"": ""2010-07-20"", ""documentnummer"": ""Z.10-8226/D.10-5338""}"
        14,0457,0457020000002632.1,2022-01-15,"{""heeftAlsHoofdadres/NummeraanduidingRef"": ""0457200000202016"", ""voorkomen/Voorkomen/voorkomenidentificatie"": ""1"", ""voorkomen/Voorkomen/beginGeldigheid"": ""2010-07-29"", ""voorkomen/Voorkomen/tijdstipRegistratie"": ""2010-11-26T08:51:39.000"", ""voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV"": ""2010-11-26T09:01:35.772"", ""identificatie"": ""0457020000002632"", ""status"": ""Plaats aangewezen"", ""geometrie"": ""POLYGON ((133347.967 478738.328,133329.722 478739.531,133329.312 478734.088,133347.614 478732.94,133347.967 478738.328))"", ""geconstateerd"": ""N"", ""documentdatum"": ""2010-07-20"", ""documentnummer"": ""Z.10-8226/D.10-5338""}"
        15,0457,0457020000002634.1,2022-01-15,"{""heeftAlsHoofdadres/NummeraanduidingRef"": ""0457200000202019"", ""voorkomen/Voorkomen/voorkomenidentificatie"": ""1"", ""voorkomen/Voorkomen/beginGeldigheid"": ""2010-07-29"", ""voorkomen/Voorkomen/tijdstipRegistratie"": ""2010-11-26T08:51:39.000"", ""voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV"": ""2010-11-26T09:01:35.796"", ""identificatie"": ""0457020000002634"", ""status"": ""Plaats aangewezen"", ""geometrie"": ""POLYGON ((133401.359 478733.295,133382.486 478734.362,133381.953 478725.673,133400.62 478724.597,133401.359 478733.295))"", ""geconstateerd"": ""N"", ""documentdatum"": ""2010-07-20"", ""documentnummer"": ""Z.10-8226/D.10-5338""}"
        16,0457,0457020000002635.1,2022-01-15,"{""heeftAlsHoofdadres/NummeraanduidingRef"": ""0457200000202020"", ""voorkomen/Voorkomen/voorkomenidentificatie"": ""1"", ""voorkomen/Voorkomen/beginGeldigheid"": ""2010-07-29"", ""voorkomen/Voorkomen/tijdstipRegistratie"": ""2010-11-26T08:51:39.000"", ""voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV"": ""2010-11-26T09:01:35.803"", ""identificatie"": ""0457020000002635"", ""status"": ""Plaats aangewezen"", ""geometrie"": ""POLYGON ((133422.366 478731.435,133403.082 478731.927,133402.836 478724.788,133422.174 478724.132,133422.366 478731.435))"", ""geconstateerd"": ""N"", ""documentdatum"": ""2010-07-20"", ""documentnummer"": ""Z.10-8226/D.10-5338""}"
        17,0457,0457020000002637.1,2022-01-15,"{""heeftAlsHoofdadres/NummeraanduidingRef"": ""0457200000202022"", ""voorkomen/Voorkomen/voorkomenidentificatie"": ""1"", ""voorkomen/Voorkomen/beginGeldigheid"": ""2010-07-29"", ""voorkomen/Voorkomen/tijdstipRegistratie"": ""2010-11-26T08:51:39.000"", ""voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV"": ""2010-11-26T09:01:35.808"", ""identificatie"": ""0457020000002637"", ""status"": ""Plaats aangewezen"", ""geometrie"": ""POLYGON ((133534.177 478739.079,133517.761 478734.193,133519.522 478728.275,133535.938 478733.161,133534.177 478739.079))"", ""geconstateerd"": ""N"", ""documentdatum"": ""2010-07-20"", ""documentnummer"": ""Z.10-8226/D.10-5338""}"
        """
        object_data = "{""heeftAlsHoofdadres/NummeraanduidingRef"": ""0457200000202010"", ""voorkomen/Voorkomen/voorkomenidentificatie"": ""1"", ""voorkomen/Voorkomen/beginGeldigheid"": ""2010-07-29"", ""voorkomen/Voorkomen/tijdstipRegistratie"": ""2010-11-26T08:51:38.000"", ""voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV"": ""2010-11-26T09:01:35.742"", ""identificatie"": ""0457020000002626"", ""status"": ""Plaats aangewezen"", ""geometrie"": ""POLYGON ((133187.488 478733.361,133188.643 478725.651,133206.945 478728.392,133205.79 478736.102,133187.488 478733.361))"", ""geconstateerd"": ""N"", ""documentdatum"": ""2010-07-20"", ""documentnummer"": ""Z.10-8226/D.10-5338""}"
        print(database.execute(
            "SELECT * FROM bag_ligplaatsen"
        ).all())

        database.execute(
            f"INSERT INTO bag_ligplaatsen (id, gemeente, object_id, last_update, object) VALUES "
            f"('8', '0457', ',0457020000002626.1', '2022-01-15', '\"{object_data}\"')"
        )
        database.commit()

        # cursor = database.execute("""
        # SELECT * FROM pg_catalog.pg_tables
        # WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'
        # """)
        # print(cursor.all())
        handle_import_msg(msg)
