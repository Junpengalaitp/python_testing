import unittest
from datetime import datetime

from database.mysqlmanager import SentimentDatabaseManager as DB


class Daily12hTest(unittest.TestCase):

    def setUp(self) -> None:
        query = "SELECT QUOTEID,SYMBOL, CONCAT(QUOTE_DATE,' ',QUOTIM) AS TIME FROM daily_10m ORDER BY TIME DESC LIMIT 7"
        self.time_interval = 600
        self.recent_7_ticks = DB(1).get_data_from_db(query)
        self.tick_times = self.recent_7_ticks['TIME'].values.tolist()

    def test_recent_7_ticks_integrity(self):
        """Test inserted recent data contains all 7 currency"""
        is_consistent = all(x == self.tick_times[0] for x in self.tick_times)
        self.assertTrue(is_consistent)

    def test_recent_7_ticks_is_inserted(self):
        """Test lasted data inserted time from now is less than time interval"""
        latest_insert_time = datetime.strptime(self.tick_times[0], "%Y-%m-%d %H:%M:%S")
        self.assertTrue((datetime.now() - latest_insert_time).total_seconds() < self.time_interval)






