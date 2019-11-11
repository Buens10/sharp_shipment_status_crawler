import logging
import numpy as np
from functools import reduce
import requests
from lxml import etree
import json
import datetime
import time
from concurrent import futures
import concurrent
from concurrent.futures import ThreadPoolExecutor as PoolExecutor


# Gets or creates a logger
logger = logging.getLogger('scrabu_app')
# set log level
logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='%I:%M:%S') # level=logging.INFO

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

#logging.basicConfig(formatter)



# Generate a list of shipment numbers with the check digit calculation
def generate_shipment_numbers(shipment_number=None, size=2): #340434188193324407|340434188193323500
    logger.info("Generating shipment numbers with seed: {}".format(shipment_number))
    multiplier = [3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3]
    shipment_numbers_list = []
    for i in range(0, size):
        shipment_number = (shipment_number // 10) + 1
        shipment_number_l = list(map(int, str(shipment_number)))
        multiply_number = np.multiply(multiplier, shipment_number_l)
        sum = reduce(lambda x, y: x+y, multiply_number)
        pz = (10 - sum % 10)
        shipment_number_l.append(0 if pz==10 else pz)
        shipment_number = reduce(lambda x,y: x * 10 + y, shipment_number_l)
        shipment_number_str = str(shipment_number).rjust(20, '0')
        shipment_numbers_list.append(shipment_number_str)
    logger.info("Generated {} unique shipment numbers".format(len(set(shipment_numbers_list))))
    return shipment_numbers_list


# Download the HTML content for a list of shipment numbers
def request(shipment_number=None, start_url="https://www.dhl.de/int-verfolgen/search?language=de&lang=de&domain=de&piececode="):
    logger.debug("Making HTTP request for shipment number {}".format(shipment_number))
    request_url = start_url + str(shipment_number)
    return requests.get(request_url).content


# Parsing HTML and converting it into JSON
def html_to_json(html):
    logger.debug("Converting HTML to JSON")
    
    html_tree = etree.HTML(html)
    
    def clean_json(dirty_json):
        start = dirty_json.find('JSON.parse(')
        end = dirty_json.find('"),', start)
        cjson = dirty_json[start:end]
        cjson = cjson.replace('JSON.parse("', '')
        cjson = cjson.replace('\\', '')
        return cjson
    
    def find_json_element(html_tree):
        json_element = html_tree.xpath('//div')
        return str(etree.tostring(json_element[0]))
    
    dirty_json = find_json_element(html_tree)
    json_string = clean_json(dirty_json)
    return json.loads(json_string)


# Structure the shipment details in JSON format
def shipment_details(shipment_details_json, start_url="https://www.dhl.de/int-verfolgen/search?language=de&lang=de&domain=de&piececode=", shipment_number="340434188193323500"):
    logger.debug("Preparing JSON for persistance")
    delivery_history_dict = {}
    delivery_history_dict["shipment_number"] = shipment_details_json["sendungen"][0]["sendungsdetails"]
    ["sendungsnummern"].get("sendungsnummer")
    delivery_history_dict["crawltime"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    delivery_history_dict["url"] = start_url + str(shipment_number) 
    delivery_history_dict["events"] = shipment_details_json["sendungen"][0]["sendungsdetails"]
    ["sendungsverlauf"].get("events", [])
    return delivery_history_dict
                     
                   
def save_dictionary(shipment_history=None, filename=None):
    logger.info("Writing file {}".format(filename))
    with open(filename, 'w') as f:
        json.dump(shipment_history, f)
                     
                     
# This function summarizes all previous steps for a single shipment number
def process_shipment_number(shipment_number):
    time.sleep(1.0)
    html_response = request(shipment_number=shipment_number)
    shipment_details_json = html_to_json(html_response)
    shipment_history = shipment_details(shipment_details_json, shipment_number=shipment_number)
    if len(shipment_history['events']) > 0:
        save_dictionary(shipment_history, filename="../data/{}.json".format(shipment_number))
    else:
        logger.info("No events found for shipment number {}".format(shipment_number))
                     
                     
# Multi-threading
def main(shipment_number=None, size=None, max_workers=None, start_url="https://www.dhl.de/int-verfolgen/search?language=de&lang=de&domain=de&piececode="):
    shipment_numbers = generate_shipment_numbers(shipment_number=shipment_number, size=size)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(process_shipment_number, sn): sn for sn in shipment_numbers}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (url, exc))
         
         
if __name__ == '__main__': 
         main(shipment_number=340434188193324407, size=10000, max_workers=4)


