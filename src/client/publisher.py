import pika

def publish(config, data):
    connection, channel = _connect(config)
    # _publish_data(channel, data)
    _disconnect(connection)


def _connect(config):
    address = config["connection_address"]
    queue = config["queue"]

    connection = pika.BlockingConnection(pika.ConnectionParameters(address))
    channel = connection.channel()
    channel.queue_declare(queue)
    return connection, channel


def _publish_data(channel, data):
    channel.basic_publish(exchange='',
                          routing_key='',
                          body=data)


def _disconnect(connection):
    connection.close()
