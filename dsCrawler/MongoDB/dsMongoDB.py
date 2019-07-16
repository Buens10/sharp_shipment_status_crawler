from pprint import pprint
from dsCrawler.spiders.dhlSpider import *


client = MongoClient("mongodb://localhost:27017/")
db = client['ds_shipment_tracking']
shipment_numbers = db['shipment_numbers']
dhl_shipment_events = db['dhl_shipment_events']
carriers = db['carriers']


shipment_number1 = {"carrier_name": "dhl", "sn": "422845989427"}
shipment_number2 = {"carrier_name": "dhl", "sn": "572760507969"}
shipment_number3 = {"carrier_name": "dhl", "sn": "00340434174857037035"}

result = shipment_numbers.insert_one(shipment_number1)
result = shipment_numbers.insert_one(shipment_number2)
result = shipment_numbers.insert_one(shipment_number3)

shipment_numbers = shipment_numbers.find({})
for shipment_number in shipment_numbers:
    pprint(shipment_number)


carrier1 = {"carrier_name": "dhl",
            "carrier_url": "https://www.dhl.de/int-verfolgen/search?language=de&lang=de&domain=de&piececode="}

result = carriers.insert_one(carrier1)

carriers = carriers.find({})
for carrier in carriers:
    pprint(carrier)




