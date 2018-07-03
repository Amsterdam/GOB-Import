import json
import pika
import atexit

from config import config

_connection = None
_channel = None


def connect():
    '''
    Connect to the RabbitMQ message broker.
    The connection is automatically closed at the end of the program

    :return: None
    '''
    global _connection
    global _channel

    _connection = pika.BlockingConnection(pika.ConnectionParameters(config["publisher"]["connection_address"]))
    _channel = _connection.channel()

    atexit.register(_disconnect)


def publish(queue, key, msg):
    '''
    Publish a message on a message queue with the specific key

    :param queue: name of the queue
    :param key: identifying key
    :param msg: message object (this will be translated to json before publishing it on the bus)
    :return: None
    '''
    if _channel is None:
        connect()

    json_msg = json.dumps(msg)

    _channel.basic_publish(
        exchange=queue,
        routing_key=key,
        properties=pika.BasicProperties(
            delivery_mode=2 # Make messages persistent
        ),
        body=json_msg
    )


def _disconnect():
    '''
    Disconnect from the RabbitMQ message broker

    :return: None
    '''
    global _connection
    global _channel

    _connection.close()

    _connection = None
    _channel = None

    atexit.unregister(_disconnect)
