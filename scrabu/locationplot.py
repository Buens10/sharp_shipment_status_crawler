import datetime
from operator import itemgetter
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost:27017/")
db = client['ds_shipment_tracking']
dhl_shipment_events = db.dhl_shipment_events

# dataset
dayt = db.dhl_shipment_events.find({}, {"events.datum": 1})
orte = db.dhl_shipment_events.find({}, {"events.ort": 1})


daytime_distribution = []
for i in dayt:
    dt = i["events"]

    for j in dt:
        datum = j["datum"]
        #weekday = datetime.strptime(datum[:-6], '%Y-%m-%dT%H:%M:%S').strftime('%a')
        daytime = datetime.strptime(datum[:-6], '%Y-%m-%dT%H:%M:%S').strftime('%H:%M')
        #weekday_distribution.append(weekday)
        daytime_distribution.append(daytime)


location_distribution = []
for m in orte:
    locations = m["events"]
    locations = [i for i in locations if i]

    for n in locations:
        ort = n["ort"]
        location_distribution.append(ort)
        #print(location_distribution)


ld_distribution = list(zip(location_distribution, daytime_distribution))
ld_distribution.sort(key=lambda x: x[1])
#print(ld_distribution)

fig = go.Figure(
    data=go.Scatter(x=[i[0] for i in ld_distribution], y=[i[1] for i in ld_distribution], mode='markers'),
    layout=go.Layout(
        title='Distribution of events',
        xaxis_title='Location',
        yaxis_title='Daytime (24h)',
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False),
    )
)

fig.show()



