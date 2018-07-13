import logging
import datetime

from client.message_broker import publish
from client.config import LOGFORMAT, LOGLEVEL


class RequestsHandler(logging.Handler):
    def __init__(self, config):
        super().__init__()
        self.config = config

    def emit(self, record):
        """Emits a log record on the message broker

        :param record: log record
        :return: None
        """
        log_msg = {
            "timestamp": datetime.datetime.now().replace(microsecond=0).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "msg": record.msg,
            "formatted_msg": self.format(record)
        }

        publish(
            config=self.config,
            queue=self.config['queue'],
            key=record.levelname,
            msg=log_msg
        )


def get_logger(config, name):
    """Returns a logger instance

    :param name: The name of the logger instance. This name will be part of every log record
    :return: logger
    """
    logger = logging.getLogger(name)

    logging.basicConfig(
        level=LOGLEVEL
    )

    handler = RequestsHandler(config)
    formatter = logging.Formatter(LOGFORMAT)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
