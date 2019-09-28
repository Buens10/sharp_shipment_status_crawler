# -*- coding: utf-8 -*-
import scrapy
from scrapy_splash import SplashRequest
from ds_crawler.items import HermescrawlerItem
import json
from pymongo import MongoClient
from scrapy.crawler import CrawlerRunner


# diese class ist der Spider, mit den standardmäßigen Funktionen, von der alle anderen Spider erben
class HermesSpider(scrapy.Spider):
    name = "hermesSpider"
    allowed_domains = ["hermes.de"]
    start_urls = [
        "https://www.myhermes.de/empfangen/sendungsverfolgung/sendungsinformation/#",
    ]

    client = MongoClient("mongodb://localhost:27017/")
    db = client['ds_shipment_tracking']
    hermes_shipment_events = db.hermes_shipment_events
    shipment_numbers = db.shipment_numbers
    carriers = db.carriers

    # diese methode wird vom Spider zum crawlen der Seiten geöffnet, wenn der Spider gestartet wurde
    def start_requests(self):
        shipment_numbers = self.db['shipment_numbers']
        carriers = self.db['carriers']
        for shipment_number in shipment_numbers.find({"carrier_name": "hermes"}):
            c_name = shipment_number["carrier_name"]
            carrier = carriers.find_one({"carrier_name": c_name})
            url = carrier["carrier_url"] + shipment_number["sn"]
            yield SplashRequest(url=url, meta=shipment_number, endpoint='http://localhost:8050/render.html?',
                                callback=self.parse)

    # die parse Methode ist für die Verarbeitung der requests und das Zurückgeben
    # der gescrapten Daten/URLs verantwortlich
    # callback wird verwendet, wenn es keine responses auf requests gibt
    def parse(self, response):
        date = response.xpath('//*[@class="m-parcelhistory-date"]/node()').extract()
        print(date)
        status = response.xpath('//*[@class="m-parcelhistory-status"]/node()').extract()
        print(status)
        #date_status = response.xpath('//tr/td[@class="m-parcelhistory-date" or @class="m-parcelhistory-status"]/node()').extract()
        #print(date_status)
        date_status = dict(zip(date, status))
        print(date_status)

#runner = CrawlerRunner()
#runner.crawl(HermesSpider)

