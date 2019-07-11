import unittest
from datetime import datetime, timedelta

from database.mysqlmanager import SentimentDatabaseManager as DB


class TestTickForGivenDay(unittest.TestCase):

    def setUp(self) -> None:
        self.date = None
        # Default date is last workday
        if not self.date:
            self.date = datetime.now().date()
            self.date -= timedelta(days=1)
            while self.date.weekday() > 4:
                self.date -= timedelta(days=1)

    def test_daily12h_is_complete(self):
        query = f"SELECT * FROM daily_12h WHERE QUOTE_DATE = '{self.date}'"
        total_ticks = DB(1).get_data_from_db(query)
        self.assertEqual(14, total_ticks.__len__())

    def test_daily6h_is_complete(self):
        query = f"SELECT * FROM daily_6h WHERE QUOTE_DATE = '{self.date}'"
        total_ticks = DB(1).get_data_from_db(query)
        self.assertEqual(28, total_ticks.__len__())

    def test_daily4h_is_complete(self):
        query = f"SELECT * FROM daily_4h WHERE QUOTE_DATE = '{self.date}'"
        total_ticks = DB(1).get_data_from_db(query)
        self.assertEqual(35, total_ticks.__len__())

    def test_daily1h_is_complete(self):
        query = f"SELECT * FROM daily_1h WHERE QUOTE_DATE = '{self.date}'"
        total_ticks = DB(1).get_data_from_db(query)
        self.assertEqual(168, total_ticks.__len__())

    def test_daily30m_is_complete(self):
        query = f"SELECT * FROM daily_30m WHERE QUOTE_DATE = '{self.date}'"
        total_ticks = DB(1).get_data_from_db(query)
        self.assertEqual(336, total_ticks.__len__())

    def test_daily10m_is_complete(self):
        query = f"SELECT * FROM daily_10m WHERE QUOTE_DATE = '{self.date}'"
        total_ticks = DB(1).get_data_from_db(query)
        self.assertEqual(1008, total_ticks.__len__())

    def test_daily5m_is_complete(self):
        query = f"SELECT * FROM daily_5m WHERE QUOTE_DATE = '{self.date}'"
        total_ticks = DB(1).get_data_from_db(query)
        self.assertEqual(2016, total_ticks.__len__())

    def test_daily1m_is_complete(self):
        query = f"SELECT * FROM daily_5m WHERE QUOTE_DATE = '{self.date}'"
        total_ticks = DB(1).get_data_from_db(query)
        self.assertEqual(10080, total_ticks.__len__())

    def test_daily30s_is_complete(self):
        query = f"SELECT * FROM daily_30s WHERE QUOTE_DATE = '{self.date}'"
        total_ticks = DB(1).get_data_from_db(query)
        self.assertEqual(20160, total_ticks.__len__())






