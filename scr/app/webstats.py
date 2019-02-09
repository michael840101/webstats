#!/usr/bin/python
# -*- coding: utf-8 -*-

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import psycopg2
import os
from dash.dependencies import Input, Output

# PostgreSQL connection setting

db_uer = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASS')
mode = 'append'
url_connect = \
    'jdbc:postgresql://ec2-3-92-151-139.compute-1.amazonaws.com:5432/cc_dev'
properties = {'user': db_uer, 'password': db_password,
              'driver': 'org.postgresql.Driver'}

connection_parameters = {
    'host': 'ec2-3-92-151-139.compute-1.amazonaws.com',
    'database': 'cc_dev',
    'user': db_uer,
    'password': db_password,
    }

# create db connection session

conn = psycopg2.connect(**connection_parameters)
cur = conn.cursor()
cur.execute("SELECT domain, url_increased,url_decreased  FROM domains \
             LIMIT 10"
            )
url_ct = cur.fetchall()

# print(url_ct)
# cur.execute("SELECT domain FROM cc_url")
# domain1=cur.fetchall()


xData = [ct[0] for ct in url_ct]

yData = [ct[1] for ct in url_ct]

y2Data=[ct[2] for ct in url_ct]

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(children='WEBSTATS'),
    html.H2(children='Page Creation Metric Cross Web.'),
    html.Div(dcc.Input(id='input-box', type='text')),
    html.Button('Submit', id='button'),
    html.Div(id='output-container-button',
             children='Enter a domain name and press submit for details'),
    html.Div(children='The rank of sites are based on content volume.'),
    dcc.Graph(id='web-active-graph',
              figure=go.Figure(data=[go.Bar(x=xData, y=yData,
              name='Page created',
              marker=go.bar.Marker(color='rgb(55, 83, 109)')),
              go.Bar(x=xData, y=y2Data, name='Page deleted',
              marker=go.bar.Marker(color='rgb(232, 117, 85)'))],
              layout=go.Layout(title='Top 1o most high volume domains',
              showlegend=True, legend=go.layout.Legend(x=0, y=1.0),
              margin=go.layout.Margin(l=40, r=0, t=40, b=30)))),
    ])


# Controller functions for the call back

@app.callback(dash.dependencies.Output('web-active-graph',
              'figure'), [dash.dependencies.Input('button', 'n_clicks'
              )], [dash.dependencies.State('input-box', 'value')])
def update_figure(n_clicks, value):

    SQL = "SELECT domain, url_total, url_increased,url_decreased FROM domains \
            WHERE domain= (%s);"
    print (value)
    input_data=(value,)
    cur = conn.cursor()
    cur.execute(SQL, input_data)
    url_ct = cur.fetchall()
    print (url_ct)
    dn=(url_ct[0][0],)
    url_tt=(url_ct[0][1],)
    url_in=(url_ct[0][2],)
    url_de=(url_ct[0][3],)
    figure_single=go.Figure(data=[go.Bar(x=dn, y=url_tt,
    name='Total Page',
    marker=go.bar.Marker(color='rgb(128, 129, 132)')),
    go.Bar(x=dn, y=url_in, name='Page created',
    marker=go.bar.Marker(color='rgb(55, 83, 109)')),
    go.Bar(x=dn, y=url_de, name='Page deleted',
    marker=go.bar.Marker(color='rgb(232, 117, 85)'))],
    layout=go.Layout(title='Monthly Page Creation/Deletion Metrics',
    showlegend=True, legend=go.layout.Legend(x=0, y=1.0),
    margin=go.layout.Margin(l=40, r=0, t=40, b=30)))
    return figure_single


if __name__ == '__main__':
    app.run_server(host='0.0.0.0', debug=True)
