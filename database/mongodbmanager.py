import logging
from datetime import timedelta

from pymongo import DESCENDING
from pymongo import MongoClient

from logger.Log import setup_logging
from utils import read_conf

setup_logging()
logger = logging.getLogger("dbLogger")


class MongoDBManager:
    def __init__(self, client=None, expires=timedelta(days=30)):
        """
        client: mongo database client
        expires: timedelta of amount of time before a cache entry is considered expired
        """
        # if a client object is not passed
        # then try connecting to mongodb at the default localhost port
        mongo_ip = read_conf.get_config('mongodb', 'ip')
        mongo_port = int(read_conf.get_config('mongodb', 'port'))

        self.client = MongoClient(mongo_ip, mongo_port) if client is None else client

        # create collection to store cached webpages,
        # which is the equivalent of a table in a relational database
        self.db = self.client.spider

        # create index if db is empty
        if self.db.crawler_news.count() is 0:
            self.db.crawler_news.create_index([("crawled_time", DESCENDING)])

    def find_social_media(self, id):
        # when same url, get last news
        tweet = self.db.social_media.find({"_id": id})

        if tweet.count() == 0:
            logger.info('No record found for News ID %s' % id)
            return None

        return tweet[0]

    def find_news(self, id):
        # when same url, get last news
        news = self.db.mrd.find({"_id": id})
        if news.count() == 0:
            logger.info('No record found for News ID %s' % id)
            return None

        return news[0]

    def get_news_time(self, id, source):
        if 'TW' == source:
            news = self.find_social_media(id)
            return news['pub_date']
        else:
            news = self.find_news(id)
            return news['crawled_time']

        return None


if __name__ == '__main__':
    manager = MongoDBManager()
