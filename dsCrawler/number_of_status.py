from pymongo import MongoClient
from itertools import chain
import re


def cleanhtml(raw_html):
    cleantext = raw_html
    if isinstance(raw_html, str):
        cleanr = re.compile('<.*a>')
        cleantext = re.sub(cleanr, '', raw_html)
    return cleantext


def cleandate(raw_html):
    cleaneddate = raw_html
    if isinstance(raw_html, str):
        cleandt = re.compile(r'([0-3][0-9])\.([0-9]{2})\.([0-9]{4})')
        cleaneddate = re.sub(cleandt, '', raw_html)
    return cleaneddate


client = MongoClient("mongodb://localhost:27017/")
db = client['ds_shipment_tracking']
dhl_shipment_events = db.dhl_shipment_events

res = dhl_shipment_events.find()


def cleanstatusz(res):
    t = list(map(lambda x: list(map(lambda y: y.get('status'), x.get('events'))), res))
    c = list(chain.from_iterable(t))#stati
    f = list(map(lambda x: cleanhtml(x), c))
    cdt = list(map(lambda x: cleandate(x), f))
    #s = set(cdt)

    #cleanlist = []
    counts = {}
    for status in cdt:
        if status in counts:
            counts[status] = counts[status]+1
        else:
            counts[status] =1
    #print(counts)


cleanstatusz(res)


