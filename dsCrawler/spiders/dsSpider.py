# -*- coding: utf-8 -*-
import scrapy
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy_splash import SplashRequest
import json


# diese class ist der Spider, mit den standardmäßigen Funktionen, von der alle anderen Spider erben
class DsSpider(scrapy.Spider):
    name = "delivery_services"
    allowed_domains = ['dhl.de']
    start_urls = ['https://www.dhl.de/de/privatkunden/pakete-empfangen/verfolgen.html?']

    # diese methode wird vom Spider zum crawlen der Seiten geöffnet, wenn der Spider gestartet wurde
    def start_requests(self):
        numbers = ["422845989427", "572760507969"]
        for number in numbers:
            url = 'https://www.dhl.de/int-verfolgen/search?language=de&lang=de&domain=de&piececode=' + number
            yield SplashRequest(url, endpoint='http://localhost:8050/render.html?', callback=self.parse)

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
        #printen des inhalts zwischen diesen punkten
        print(start)
        print(end)

        #anpassen des strings
        json_string = string[start:end]
        json_string = json_string.replace('JSON.parse("', '')
        json_string = json_string.replace('\\', '')
        shipment_details = json.loads(json_string)

        #definition des gesuchten pfades
        events = shipment_details["sendungen"][0]["sendungsdetails"]["sendungsverlauf"]["events"]
        for event in events:
            print(event["datum"]+": "+event["status"])

#runner
runner = CrawlerRunner()
runner.crawl(DsSpider)
d = runner.join()
d.addBoth(lambda _: reactor.stop())

reactor.run()

print("delivery-services_crawler")