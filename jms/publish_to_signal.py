import logging

from jms.publish_to_mq import publish_to_mq
from logger.Log import setup_logging
from utils import read_conf

setup_logging()
logger = logging.getLogger("fileLogger")


def publish_to_marco_signal_mq(publish_data):
    try:
        # Read IP, Port and Topic details from config.ini
        ip = read_conf.get_config('CONFIG_MQ', 'IP')
        port = int(read_conf.get_config('CONFIG_MQ', 'PORT'))
        topic = read_conf.get_config('CONFIG_MQ', 'SIGNAL_TOPIC')
        # publish details to ActiveMQ
        publish_to_mq(ip, port, topic, publish_data)
    except Exception as e:
        logger.error(
            'Exception occurred while publishing message to MQ. : "%s"' % publish_data)
        logger.error(e)


def publish_to_news_signal_mq(publish_data):
    try:
        # Read IP, Port and Topic details from config.ini
        ip = read_conf.get_config('CONFIG_MQ', 'IP')
        port = int(read_conf.get_config('CONFIG_MQ', 'PORT'))
        topic = read_conf.get_config('CONFIG_MQ', 'NEWS_SIGNAL_TOPIC')
        # publish details to ActiveMQ
        publish_to_mq(ip, port, topic, publish_data)
    except Exception as e:
        logger.error(
            'Exception occurred while publishing message to MQ. : "%s"' % publish_data)
        logger.error(e)