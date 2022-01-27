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
        handle_import_msg(msg)
