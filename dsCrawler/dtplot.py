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
weekday_distribution = []
daytime_distribution = []
#print(wd_distribution)
for i in dayt:
    dt = i["events"]

    for j in dt:
        datum = j["datum"]
        weekday = datetime.strptime(datum[:-6], '%Y-%m-%dT%H:%M:%S').strftime('%a')
        daytime = datetime.strptime(datum[:-6], '%Y-%m-%dT%H:%M:%S').strftime('%H:%M')
        weekday_distribution.append(weekday)
        daytime_distribution.append(daytime)
#print(weekday_distribution)
#print(daytime_distribution)
wd_distribution = list(zip(weekday_distribution, daytime_distribution))
wd_distribution.sort(key=lambda x: x[1])
print(wd_distribution)


fig = go.Figure(
    data=go.Scatter(x=[i[0] for i in wd_distribution], y=[i[1] for i in wd_distribution], mode='markers'),
    layout=go.Layout(
        title='Distribution of events',
        xaxis_title='Weekdays',
        yaxis_title='Daytime (24h)',
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False),
    )
)

fig.show()


