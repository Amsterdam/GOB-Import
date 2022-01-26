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
        handle_import_msg(msg)

        # See: test_handle_import_msg
        # service = 'import_request': {
        #     'queue': IMPORT_QUEUE,
        #     'handler': handle_import_msg,  #from: __main__.py
        #     'report': {
        #         'exchange': WORKFLOW_EXCHANGE,
        #         'key': IMPORT_RESULT_KEY,
        #     },
        # },
        # # message maken met {"queue": "bogus"}
        # msg = {"queue": "bla"}
        # # _on_message(connection=None, msg, service)
        # handler = service["handler"]
        # result_msg = handler(msg)
        # result = _handle_result_msg(connection, service, result_msg)
        # # see


    def test_fixtures(self):
        print("pass")
