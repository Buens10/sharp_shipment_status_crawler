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


def cleanstatussen(res):
    t = list(map(lambda x: list(map(lambda y: y.get('status'), x.get('events'))), res))
    #print("List T")
    #print(t)
    c = list(chain.from_iterable(t)) #stati
    #print(c)
    f = list(map(lambda x: cleanhtml(x), c))
    cdt = list(map(lambda x: cleandate(x), f))
    s = set(cdt)

    cleanlist = []
    for status in s:
        if status is not None:
            cleanlist.append(status)

    sortedcl = sorted(cleanlist)

    for status in sortedcl:
        print(status)
        print('\n')

'''
    with open('Alle_Statis.txt', 'w') as f:
        for i in sortedcl:
            f.write("%s\n\n" % i)
'''

cleanstatussen(res)

