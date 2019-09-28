from pymongo import MongoClient
import networkx as nx
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('PS')
import plotly.graph_objects as go
import matplotlib.pyplot as plt
#from netwulf import visualize


client = MongoClient("mongodb://localhost:27017/")
db = client['ds_shipment_tracking']
dhl_shipment_events = db.dhl_shipment_events

res = dhl_shipment_events.find()


listLocation = list(map(lambda x: list(map(lambda y: y.get('ort'), x.get('events'))), res))
finList = []
for list in listLocation:
    midList = []
    for elem in list:
        if elem is not None:
            midList.append(elem)
    finList.append(midList)

locList = []
for lst in finList:
    if len(lst) > 1:
        locList.append(lst)
#print(locList)

chunks = []
for plList in locList:
    for place in range(len(plList)):
        if place < len(plList)-1:
            chunks.append([plList[place], plList[place+1]])
#print(chunks)


#countedChunks
chunkdict = {}
for plList in chunks:
    key = plList[0] + plList[1]
    chunkdict[key] = {"start": plList[0], "ziel": plList[1], "count": 0}
#print(chunkdict)

for plList in chunks:
    key = plList[0] + plList[1]
    chunkdict[key]["count"] += 1
#print(chunkdict)

sorted_a2b = sorted(chunkdict.items(), key=lambda x: x[1]['count'], reverse=True)
#print(sorted_a2b)

a = ({i[1]['start'] for i in sorted_a2b})
#print(a)
b = ({i[1]['ziel'] for i in sorted_a2b})
#print(b)
c = ({i[1]['count'] for i in sorted_a2b})

# network graph
G = nx.random_geometric_graph(200, 0.125)

# create edges
edge_x = []
edge_y = []
for edge in G.edges():
    x0, y0 = G.node[edge[0]]['pos']
    x1, y1 = G.node[edge[1]]['pos']
    edge_x.append(x0)
    edge_x.append(x1)
    edge_x.append(None)
    edge_y.append(y0)
    edge_y.append(y1)
    edge_y.append(None)

edge_trace = go.Scatter(
    x=edge_x, y=edge_y,
    line=dict(width=0.5, color='#888'),
    hoverinfo='none',
    mode='lines')

node_x = [a]
node_y = [b]
for node in G.nodes():
    x, y = G.node[node]['pos']
    node_x.append(x)
    node_y.append(y)

node_trace = go.Scatter(
    x=node_x, y=node_y,
    mode='markers',
    hoverinfo='text',
    marker=dict(
        showscale=True,
        # colorscale options
        #'Greys' | 'YlGnBu' | 'Greens' | 'YlOrRd' | 'Bluered' | 'RdBu' |
        #'Reds' | 'Blues' | 'Picnic' | 'Rainbow' | 'Portland' | 'Jet' |
        #'Hot' | 'Blackbody' | 'Earth' | 'Electric' | 'Viridis' |
        colorscale='YlGnBu',
        reversescale=True,
        color=[],
        size=10,
        colorbar=dict(
            thickness=15,
            title='Node Connections',
            xanchor='left',
            titleside='right'
        ),
        line_width=2))

# color node points
node_adjacencies = []
node_text = []
for node, adjacencies in enumerate(G.adjacency()):
    node_adjacencies.append(len(adjacencies[1]))
    node_text.append('# of connections: '+str(len(adjacencies[1])))

node_trace.marker.color = node_adjacencies
node_trace.text = node_text

# create graph
fig = go.Figure(data=[edge_trace, node_trace],
                layout=go.Layout(
                title='<br>Network graph made with Python',
                titlefont_size=16,
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20,l=5,r=5,t=40),
                annotations=[dict(
                    text="Python code: <a href='https://plot.ly/ipython-notebooks/network-graphs/'> https://plot.ly/ipython-notebooks/network-graphs/</a>",
                    showarrow=False,
                    xref="paper", yref="paper",
                    x=0.005, y=-0.002)],
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                )
fig.show()


'''
# Build a dataframe with your connections
df = pd.DataFrame({'from': a, 'to': b})
# Build your graph
G = nx.from_pandas_edgelist(df, 'from', 'to')
# Graph with Custom nodes:
nx.draw(G, with_labels=True, node_size=1500, node_color="skyblue", node_shape="s", alpha=0.5, linewidths=40)
plt.show()
'''

'''
fig = go.Figure(
    data=go.Scatter(x=[i[0] for i in locList], y=[i[1] for i in locList], mode='markers'),
    layout=go.Layout(
        title='Distribution of delivery places',
        xaxis_title='Count',
        yaxis_title='Location (a to b)',
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False),
    )
)

fig.show()
'''

