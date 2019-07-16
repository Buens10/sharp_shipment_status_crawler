# -*- coding: utf-8 -*-
import math
import scrapy
from scrapy_splash import SplashRequest
from dsCrawler.items import DhlcrawlerItem
import json
from pymongo import MongoClient
import numpy as np
from functools import reduce
from datetime import datetime
import datetime
from scrapy.crawler import CrawlerRunner


# diese class ist der Spider, mit den standardmäßigen Funktionen, von der alle anderen Spider erben
class DhlSpider(scrapy.Spider):
    name = "dhlSpider"
    allowed_domains = "dhl.de"
    start_url = "https://www.dhl.de/int-verfolgen/search?language=de&lang=de&domain=de&piececode="

    client = MongoClient("mongodb://localhost:27017/")
    db = client['ds_shipment_tracking']
    dhl_shipment_events = db.dhl_shipment_events
    carriers = db.carriers

    # diese methode wird vom Spider zum crawlen der Seiten geöffnet, wenn der Spider gestartet wurde
    def start_requests(self):
        number = 340434174857037035
        multiplier = [3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3]
        sh_numbers = []
        for i in range(1, 100):
            sh_number = (number // 10) + i
            sh_number_list = list(map(int, str(sh_number)))
            m_number = np.multiply(multiplier, sh_number_list)
            sum = reduce(lambda x, y: x+y, m_number)
            pz = (10 - sum % 10)
            sh_number_list.append(0 if pz==10 else pz)

            sh_number = reduce(lambda x, y: x * 10 + y, sh_number_list)
            sh_number = str(sh_number).rjust(20, '0')
            sh_numbers.append(sh_number)

        for sh_number in sh_numbers:
            url = self.start_url + sh_number
            yield SplashRequest(url, endpoint='http://localhost:8050/render.html?',
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
        #print(shipment_details)

        #definition des gesuchten pfades
        item = DhlcrawlerItem(
            sn=shipment_details["sendungen"][0]["sendungsdetails"]["sendungsnummern"].get("sendungsnummer"),
            crawltime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
            url=response.meta["splash"]["args"].get("url"),
            events=shipment_details["sendungen"][0]["sendungsdetails"]["sendungsverlauf"].get("events", [])
        )
        yield item



#runner = CrawlerRunner()
#runner.crawl(DhlSpider)

