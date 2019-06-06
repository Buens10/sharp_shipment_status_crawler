# -*- coding: utf-8 -*-
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy_splash import SplashRequest
from dsCrawler.items import DscrawlerItem
import json
from pymongo import MongoClient
import scrapy.crawler
from scrapy.utils.log import configure_logging


# diese class ist der Spider, mit den standardmäßigen Funktionen, von der alle anderen Spider erben
class DsSpider(scrapy.Spider):
    name = "delivery_services"
    allowed_domains = ['dhl.de']
    start_urls = [
        'https://www.dhl.de/int-verfolgen/search?language=de&lang=de&domain=de&piececode=',
    ]

    client = MongoClient("mongodb://localhost:27017/")
    db = client['ds_shipment_tracking']
    shipment_events = db.shipment_events
    shipment_numbers = db.shipment_numbers
    carriers = db.carriers

    # diese methode wird vom Spider zum crawlen der Seiten geöffnet, wenn der Spider gestartet wurde
    def start_requests(self):
        shipment_numbers = self.db['shipment_numbers']
        carriers = self.db['carriers']
        for shipment_number in shipment_numbers.find():
            c_name = shipment_number["carrier_name"]
            carrier = carriers.find_one({"carrier_name": c_name})
            url = carrier["carrier_url"] + shipment_number["sn"]
            yield SplashRequest(url, meta=shipment_number, endpoint='http://localhost:8050/render.html?',
                                callback=self.parse)

    # die parse Methode ist für die Verarbeitung der requests und das Zurückgeben
    # der gescrapten Daten/URLs verantwortlich
    # callback wird verwendet, wenn es keine responses auf requests gibt
    def parse(self, response):
        items = response.xpath('//div').extract()
        string = str(items[0])
        #start und ende des relevanten strings
        #geben den index der start- und endpunkte an
        start = string.find('JSON.parse(')
        end = string.find('"),', start)
        #anpassen/formatieren des strings
        json_string = string[start:end]
        json_string = json_string.replace('JSON.parse("', '')
        json_string = json_string.replace('\\', '')
        shipment_details = json.loads(json_string)

        #definition des gesuchten pfades
        item = DscrawlerItem(
            carrier=response.meta["carrier_name"],
            sn=response.meta["sn"],
            url=response.meta["splash"]["args"].get("url"),
            events=shipment_details["sendungen"][0]["sendungsdetails"]["sendungsverlauf"].get("events", [])
        )
        yield item


#runner
configure_logging()
runner = CrawlerRunner()
d = runner.crawl(DsSpider)
d.addBoth(lambda _: reactor.stop())

reactor.run()


print("delivery-services_crawler")

