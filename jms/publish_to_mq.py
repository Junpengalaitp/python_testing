import logging

import stomp

from logger.Log import setup_logging

setup_logging()
logger = logging.getLogger('fileLogger')


def publish_to_mq(ip, port, topic_name, publish_data):
    try:
        server = '127.0.0.1'
        if ip is not None:
            server = ip
        con = stomp.Connection([(server, port)])
        con.start()
        con.connect('admin', 'password', wait=True)
        con.send(topic_name, publish_data)
        con.disconnect()
        logger.info(
            'Result published to MQ Topic "%s" successfully and data is "%s"' % (topic_name, publish_data))
    except Exception as e:
        logger.error('Error occurred while connecting and publishing to MQ topic:"%s" for data:"%s"' % (
            topic_name, publish_data))
        logger.error(e)
