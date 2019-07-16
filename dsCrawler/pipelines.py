# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient
from scrapy.exceptions import DropItem
from scrapy.conf import settings
import dsCrawler.settings as settings
from dsCrawler.spiders import dhlSpider


class DhlShipmentEventsPipeline(object):

    collection_name = 'dhl_shipment_events'

    def __init__(self, mongo_server, mongo_port, mongo_db, mongo_collection):
        self.mongo_server = mongo_server
        self.mongo_port = mongo_port
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        #self.f=open('mongotest', 'w+')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_server=crawler.settings.get('MONGODB_SERVER'),
            mongo_port=crawler.settings.get('MONGODB_PORT'),
            mongo_db=crawler.settings.get('MONGODB_DB', 'dsCrawler'),
            mongo_collection=crawler.settings.get('MONGODB_COLLECTION'),
        )

    def open_spider(self, dhlSpider):
        self.client = MongoClient(self.mongo_server, self.mongo_port)
        self.db = self.client[self.mongo_db]

    def close_spider(self, dhlSpider):
        self.client.close()
        #self.f.close()

    def process_item(self, item, dhlSpider):
        #self.f.write(str(item))
        self.db[self.mongo_collection].insert(dict(item))
        return item


class DropIfEmptyFieldPipeline(object):

    def process_item(self, item, spider):

        if not(all(item.values())):
            raise DropItem()
        else:
            return item

