# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
import scrapy


#class DscrawlerItem(scrapy.Item):
    #pass


class DhlcrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    sn = scrapy.Field()
    crawltime = scrapy.Field()
    url = scrapy.Field()
    events = scrapy.Field()
    pass
