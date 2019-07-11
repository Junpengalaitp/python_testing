import logging
import time

import stomp

from logger.Log import setup_logging
from threadpool.SignalThreadPool import SignalThreadPool
from threadpool.MacroThreadPool import MacroThreadPool
from utils import read_conf

setup_logging()
logger = logging.getLogger("fileLogger")


class MacroEconomicTopicListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn

    def on_error(self, headers, message):
        logging.info('Received error "%s" ' % message)

    def on_message(self, headers, message):
        logging.info('Received Message "%s" ' % message)
        MacroThreadPool(message).execute()

    def on_disconnected(self):
        logging.info('Receiver Connection disconnected unexpectedly')
        logging.info('Trying to Reconnect')
        connect_and_subscribe(self.conn)


class UnStructuredDataTopicListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn

    def on_error(self, headers, message):
        logging.info('Received error "%s" ' % message)

    def on_message(self, headers, message):
        logging.info('Received Message "%s" ' % message)
        SignalThreadPool(message).execute()

    def on_disconnected(self):
        logging.info('Receiver Connection disconnected unexpectedly')
        logging.info('Trying to Reconnect')
        connect_and_subscribe(self.conn)
        

def connect_and_subscribe(conn):
    try:
        topic = read_conf.get_config('CONFIG_MQ', 'MACRO_TOPIC')

        conn.set_listener('MacroDataTopicListener', MacroEconomicTopicListener(conn))
        conn.start()
        conn.connect('admin', 'password', wait=True)
        conn.subscribe(destination=topic, id=1, ack='auto')
    except Exception as e:
        logging.info('Exception occured while connecting to MQ')
        logging.info(e)


def connect_and_subscribe_unstruct(conn2):
    try:
        topic = read_conf.get_config('CONFIG_MQ', 'HOTSPOT_TOPIC')

        conn2.set_listener('UnStructuredDataTopicListener', UnStructuredDataTopicListener(conn2))
        conn2.start()
        conn2.connect('admin', 'password', wait=True)
        conn2.subscribe(destination=topic, id=1, ack='auto')
    except Exception as e:
        logging.info('Exception occured while connecting to MQ')
        logging.info(e)


def start():
    logging.info('Starting ActiveMQ Listener')

    ip = read_conf.get_config('CONFIG_MQ', 'IP')
    port = int(read_conf.get_config('CONFIG_MQ', 'PORT'))

    logging.info('Server : ' + ip)
    logging.info('Topic : ' + '/topic/macro_data')

    conn = stomp.Connection([(ip, port)])
    connect_and_subscribe(conn)

    logging.info('Started listener successfully : macro_data')

    conn2 = stomp.Connection([(ip, port)])
    connect_and_subscribe_unstruct(conn2)

    logging.info('Started listener successfully : unstructured_data')

    while True:
        time.sleep(20)
    conn.disconnect()
