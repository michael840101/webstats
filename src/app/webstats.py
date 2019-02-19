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
cur.execute("SELECT domain, url_total, url_increased,url_decreased  FROM domains_curr \
             ORDER BY pg_rank LIMIT 10"
            )
url_ct = cur.fetchall()

xData = [ct[0] for ct in url_ct]

yData = [ct[1] for ct in url_ct]

y2Data = [ct[2] for ct in url_ct]

y3Data = [ct[3] for ct in url_ct]


#Structure of the html layout
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = \
    html.Div(style={
             'background-image': 'url("/assets/floor-1.jpg")',
             'background-color': 'rgb(230, 243, 247)', 'margin':'10px 80px 15px 80px'},
    children=[
    #html.Header(children='Webstats'),
    html.H1(children='WebStats', style={'textAlign': 'center',
            'font-family': 'Verdana', 'color': 'rgb(40, 34, 104)'}),


    html.Div(id='output-container-button',style={'textAlign': 'left', 'font-size':'18',
            'font-family': 'Verdana', 'color': 'rgb(40, 34, 104)','margin':'0 0 0 200px'},
             children=['Find Website Page Statistics ',
             html.Span(dcc.Input(id='input-box', type='text')),
             html.Button('Find', id='button',style={'textAlign': 'right','font-size':'18','margin':'10px 0 10px 0'})
             ]
             ),
    dcc.Graph(id='web-active-graph',
              figure=go.Figure(data=[go.Bar(x=xData, y=yData,
              name='Total pages',
              marker=go.bar.Marker(color='rgb(161, 186, 184)')),
              go.Bar(x=xData, y=y2Data, name='Pages created',
              marker=go.bar.Marker(color='rgb(161, 193, 145)')),
              go.Bar(x=xData, y=y3Data, name='Pages deleted',
              marker=go.bar.Marker(color='rgb(232, 117, 85)'))],
              layout=go.Layout(title='Top 10 domains by page volume '
              , showlegend=True, legend=go.layout.Legend(x=0, y=1.0),
              margin=go.layout.Margin(l=20, r=20, t=120, b=60)))),
    html.Footer(children='Copyright @ 2019 Zhiqin Chen All rights reserved',style={'textAlign': 'center', 'font-family': 'Verdana',
    'font-size':'10', 'color': 'rgb(40, 34, 104)'})
    ])


# Controller functions for the call back

@app.callback(dash.dependencies.Output('web-active-graph', 'figure'),
              [dash.dependencies.Input('button', 'n_clicks')],
              [dash.dependencies.State('input-box', 'value')])
def update_figure(n_clicks, value):

    SQL = \
        "SELECT domain, url_total, url_increased,url_decreased,pg_rank FROM domains_curr \
            WHERE domain= (%s);"
    SQL_1 = \
        "SELECT domain, url_total, url_increased,url_decreased,pg_rank FROM domains \
            WHERE domain= (%s);"
    print value
    input_data = (value, )
    cur = conn.cursor()
    cur.execute(SQL, input_data)
    url_ct = cur.fetchall()
    print url_ct
    cur_1 = conn.cursor()
    cur_1.execute(SQL_1, input_data)
    url_ct_quater = cur_1.fetchall()

    dn = ('Jan 2019', 'Q4 2018')
    url_tt = (url_ct_quater[0][1], url_ct[0][1] )
    url_in = (url_ct_quater[0][2], url_ct[0][2] )
    url_de = (url_ct_quater[0][3], url_ct[0][3] )
    pg_rk = (url_ct_quater[0][4],  url_ct[0][4] )

    figure_single = go.Figure(data=[go.Bar(x=dn, y=url_tt,
                              name='Total Pages' + '(Rank '
                              + str(pg_rk[1]) + '*)', width=0.15,
                              marker=go.bar.Marker(color='rgb(161, 186, 184)'
                              )), go.Bar(x=dn, y=url_in,
                              name='Pages created', width=0.15,
                              marker=go.bar.Marker(color='rgb(161, 193, 145)'
                              )), go.Bar(x=dn, y=url_de,
                              name='Pages deleted', width=0.15,
                              marker=go.bar.Marker(color='rgb(232, 117, 85)'
                              ))],
                              layout=go.Layout(title= 'Page Generation Metric Of '
                               + str(value), showlegend=True,
                              legend=go.layout.Legend(x=0, y=1.0),
                              margin=go.layout.Margin(l=40, r=40,
                              t=120, b=60)))
    return figure_single

#run the server
if __name__ == '__main__':
    app.run_server(host='0.0.0.0', debug=True)
