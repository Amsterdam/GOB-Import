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

    def test_handle_import_msg(self, gob_logger_manager_mock):
        msg = {
            "header": {
                "catalogue": "bag",
                "collection": "ligplaatsen",
                "application": "BAGExtract"
            }
        }
        # Dit crasht omdat er geen bag ligplaatsen table is.
        # Die komt normaal uit bagextract.
        #
        # Prepare dbs komen dan uit prepare prepare.api_import.SqlAPIImporter?
        #
        # Hoe komt import aan dat schema?
        #
        # We hebben die migratiecode niet, die staat enkel in bag ligplaatsen.
        # Hoe gaan we die db creeeren?
        #  - niet
        #  - bogus registratie, maar werkt dat met toevoegen attributen
        #  - bagextract-alembic naar gob-core?
        handle_import_msg(msg)



    def test_fixtures(self):
        print("pass")
