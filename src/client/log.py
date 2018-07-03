import logging
import datetime

from config import LOGLEVEL
from message_broker import publish

# asctime 	%(asctime)s 	Human-readable time when the LogRecord was created. By default this is of the form ‘2003-07-08 16:49:45,896’ (the numbers after the comma are millisecond portion of the time).
# created 	%(created)f 	Time when the LogRecord was created (as returned by time.time()).
# filename 	%(filename)s 	Filename portion of pathname.
# funcName 	%(funcName)s 	Name of function containing the logging call.
# levelname	%(levelname)s 	Text logging level for the message ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
# levelno 	%(levelno)s 	Numeric logging level for the message (DEBUG, INFO, WARNING, ERROR, CRITICAL).
# lineno 	%(lineno)d 	    Source line number where the logging call was issued (if available).
# module 	%(module)s 	    Module (name portion of filename).
# msecs 	%(msecs)d 	    Millisecond portion of the time when the LogRecord was created.
# message 	%(message)s 	The logged message, computed as msg % args. This is set when Formatter.format() is invoked.
# msg 	 	                The format string passed in the original logging call. Merged with args to produce message, or an arbitrary object (see Using arbitrary objects as messages).
# name 	    %(name)s 	    Name of the logger used to log the call.
# pathname 	%(pathname)s 	Full pathname of the source file where the logging call was issued (if available).
# process 	%(process)d 	Process ID (if available).
# processName 	    %(processName)s 	Process name (if available).
FORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
QUEUE = "gob.log"


class RequestsHandler(logging.Handler):


    def emit(self, record):
        '''
        Emits a log record on the message broker

        :param record: log record
        :return: None
        '''
        log_msg = {
            "timestamp": datetime.datetime.now().replace(microsecond=0).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "msg": record.msg,
            "formatted_msg": self.format(record)
        }

        publish(
            queue=QUEUE,
            key=record.levelname,
            msg=log_msg
        )


def get_logger(name):
    '''
    Returns a logger instance

    :param name: The name of the logger instance. This name will be part of every log record
    :return: logger
    '''
    format = FORMAT
    level = LOGLEVEL

    logger = logging.getLogger(name)

    logging.basicConfig(
        level=level,
        format=format
    )

    handler = RequestsHandler()
    formatter = logging.Formatter(format)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
