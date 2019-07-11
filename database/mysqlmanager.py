import logging

import mysql.connector
from mysql.connector import errorcode
from mysql.connector import pooling
import pandas as pd
import pytz

from logger.Log import setup_logging
from utils import read_conf

timezone = pytz.timezone('Singapore')

setup_logging()
logger = logging.getLogger("dbLogger")


# MySql Database operation
class SentimentDatabaseManager:
    DB_NAME = read_conf.get_config('MYSQL_110', 'dbname')
    SERVER_IP = read_conf.get_config('MYSQL_110', 'ip')

    def __init__(self, max_num_thread):
        username = read_conf.get_config('MYSQL_110', 'user')
        password = read_conf.get_config('MYSQL_110', 'password')
        try:
            cnx = mysql.connector.connect(host=self.SERVER_IP, user=username, password=password)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.error("Database does not exist")
            else:
                logger.error('Create Error ' + err.msg)
            exit(1)

        cursor = cnx.cursor()

        try:
            cnx.database = self.DB_NAME
        except mysql.connector.Error as err:
            logger.error(err)
            exit(1)
        finally:
            cursor.close()
            cnx.close()

        dbconfig = {
            "database": self.DB_NAME,
            "user": username,
            "host": self.SERVER_IP,
            "password": password,
        }
        self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool",
                                                                   pool_size=max_num_thread,
                                                                   **dbconfig)

    def get_data_from_db(self, sql_query):
        try:
            con = self.cnxpool.get_connection()
            cursor = con.cursor()
            cursor.execute(sql_query)
            df = pd.DataFrame(cursor.fetchall())

            if df.empty:
                return None
            else:
                df.columns = [row[0] for row in cursor.description]
                return df

        except mysql.connector.Error as e:
            print("Error while accessing MySQL", e)


class DashboardDatabaseManager:
    DB_NAME = read_conf.get_config('MYSQL_149', 'dbname')
    SERVER_IP = read_conf.get_config('MYSQL_149', 'ip')

    def __init__(self, max_num_thread):
        username = read_conf.get_config('MYSQL_149', 'user')
        password = read_conf.get_config('MYSQL_149', 'password')
        try:
            cnx = mysql.connector.connect(host=self.SERVER_IP, user=username, password=password)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.error("Database does not exist")
            else:
                logger.error('Create Error ' + err.msg)
            exit(1)

        cursor = cnx.cursor()

        try:
            cnx.database = self.DB_NAME
        except mysql.connector.Error as err:
            logger.error(err)
            exit(1)
        finally:
            cursor.close()
            cnx.close()

        dbconfig = {
            "database": self.DB_NAME,
            "user": username,
            "host": self.SERVER_IP,
            "password": password,
        }
        self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool",
                                                                   pool_size=max_num_thread,
                                                                   **dbconfig)

    def get_data_from_db(self, sql_query):
        try:
            con = self.cnxpool.get_connection()
            cursor = con.cursor()
            cursor.execute(sql_query)
            df = pd.DataFrame(cursor.fetchall())

            if df.empty:
                return None
            else:
                df.columns = [row[0] for row in cursor.description]
                return df

        except mysql.connector.Error as e:
            print("Error while accessing MySQL", e)

if __name__ == '__main__':
    manager = SentimentDatabaseManager(1)
