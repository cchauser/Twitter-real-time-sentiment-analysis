#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 11 13:31:14 2020

@author: cullen
"""

import dash
import flask
import time
import dash_core_components as dcc
import dash_html_components as html
import numpy as np
import pandas as pd
import plotly.graph_objs as go
import MySQLdb as mysql

from dash.dependencies import Input, Output
from datetime import datetime

DATABASE = 'production'

wdir = '/home/cullen'

with open('{}/keys/clientKeys.txt'.format(wdir)) as f:
    keys = f.readlines()

mysqlUser = keys[0].replace('\n', '')
mysqlPass = keys[1].replace('\n', '')


print('Connected to mySQL server')


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = flask.Flask(__name__)
dApp = dash.Dash(__name__, external_stylesheets=external_stylesheets, suppress_callback_exceptions=True, server = app)

dApp.title = 'Real-Time Analytics'


def tableColors(n):
    
    oddColor = 'rgb(174,218,251)'
    evenColor = 'white'
    
    tableColor = [oddColor, evenColor] * round(n/2)
    #Cut off at the length of userDF
    #Because the table displays the results with the most recent at the top we reverse the order for
    #the colors so that all of the entries retain the same colors and the newest gets the next color
    tableColor = tableColor[:n][::-1]
    return [tableColor * 5]

#TODO: Move all sql interaction functions into a new class    
def getRecentTopics():
    cnx = mysql.connect(user = mysqlUser, 
                         password = mysqlPass, 
                         host = '127.0.0.1',
                         database = DATABASE)
    cursor = cnx.cursor()
    lastUpdateQuery = '''SELECT TABLE_NAME, UPDATE_TIME FROM information_schema.tables 
                            WHERE UPDATE_TIME <> 'None' && TABLE_SCHEMA = '{}' 
                            ORDER BY UPDATE_TIME DESC'''.format(DATABASE)
    
    cursor.execute(lastUpdateQuery)
    topics = []
    for entry in cursor:
        if 'sentiment' not in entry[0]:
            continue
        if (datetime.now() - entry[1]).seconds / 3600 < 1:
            topics.append(entry[0].split('_')[0])
    cnx.close()
    return topics

def sqlSentimentSelect(topic):
    cnx = mysql.connect(user = mysqlUser, 
                         password = mysqlPass, 
                         host = '127.0.0.1',
                         database = DATABASE)
    cursor = cnx.cursor()
    dayAgoSeconds = int(time.time()) - 43200#86400
    selectQuery = '''SELECT time, negative, positive, neutral FROM {0}_sentiment
                        WHERE time > {1}'''.format(topic, dayAgoSeconds)
                        
    cursor.execute(selectQuery)
    timeSeries = []
    sentiments = []
    for item in cursor:
        t = datetime.fromtimestamp(item[0]).strftime('%Y-%m-%d %H:%M:%S')
        timeSeries.append(t)
        sentiments.append(list(item[1:]))
    cnx.close()
    return timeSeries, np.asarray(sentiments)

def sqlKeywordSelect(topic):
    cnx = mysql.connect(user = mysqlUser, 
                         password = mysqlPass, 
                         host = '127.0.0.1',
                         database = DATABASE)
    cursor = cnx.cursor()
    rollingKeywordWindowSeconds = (60*30) # 30 minute window
    minTimeSelection = int(time.time()) - rollingKeywordWindowSeconds
    
    selectQuery = '''
                    SELECT word, SUM(times_seen) as t, SUM(negative) as neg, SUM(positive) as pos, SUM(neutral) as neu
                    FROM {0}_keyword
                    WHERE time > {1}
                    GROUP BY word
                    ORDER BY t DESC
                  '''.format(topic, minTimeSelection)
    
    cursor.execute(selectQuery)
    container = []
    i = 0
    for item in cursor:
        container.append(list(item))
        i += 1
        if i == 15:
            break
    cnx.close()
    container = container[::-1]
    df = pd.DataFrame(container, columns = ['word', 'times_seen', 'negative', 'positive', 'neutral'])
    return df

def sqlLocationSelect(topic):
    cnx = mysql.connect(user = mysqlUser, 
                         password = mysqlPass, 
                         host = '127.0.0.1',
                         database = DATABASE)
    cursor = cnx.cursor()
    rollingKeywordWindowSeconds = (60*30) # 30 minute window
    minTimeSelection = int(time.time()) - rollingKeywordWindowSeconds
    
    selectQuery = '''
                    SELECT location, AVG(sentiment) as s, SUM(times_seen) as t
                    FROM {0}_location
                    WHERE time > {1}
                    GROUP BY location
                  '''.format(topic, minTimeSelection)
    
    cursor.execute(selectQuery)
    container = []
    for item in cursor:
        item = list(item)
        item[2] = 'Number of Tweets: {}'.format(item[2])
        container.append(item)
        
    cnx.close()
    df = pd.DataFrame(container, columns = ['location', 'sentiment', 'text'])
    return df
    
def sqlUserSelect(topic):
    cnx = mysql.connect(user = mysqlUser, 
                         password = mysqlPass, 
                         host = '127.0.0.1',
                         database = DATABASE)
    cursor = cnx.cursor()
    
    dayAgoSeconds = int(time.time()) - 43200#86400
    selectQuery = '''
                  SELECT time, user, tweet, deltasentiment, deltaactivity
                  FROM {0}_user
                  WHERE time > {1}
                  ORDER BY time DESC
                  '''.format(topic, dayAgoSeconds)
    
    cursor.execute(selectQuery)
    cnx.close()
    container = []
    annotations = []
    shapes = []
    for item in cursor:
        item = list(item)
        item[0] = datetime.fromtimestamp(item[0]).strftime('%Y-%m-%d %H:%M:%S')
        container.append(item)
        shapes.append({'x0':item[0], 'x1':item[0], 'y0':0, 'y1': 1, 'xref':'x', 'yref':'paper', 
                       'opacity': .35, 'line':{'width':.5}})
    
        # Hacker mode: engaged
        # Because shapes don't have hovertext and we want to only be able to see the text when the shape is hovered
        # we create an annotation the text set to spaces to run the length of the shape and have the hovertext 
        # set to the text that we want to display. :sunglasses:
        text = ''
        tweet = item[2].split()
        for i in range(len(tweet)):
            if i % 6 == 5:
                text += '<br>' + tweet[i]
            else:
                text += ' ' + tweet[i]
        text = text.strip()
        annotations.append({'x':item[0], 'y':0, 'xref':'x', 'height': 1, 'yref':'paper', 'showarrow':False, 
                            'xanchor':'center', 'text': ' ' * 200, 'font': {'size':7}, 'textangle': 270,
                            'hovertext':'{} tweet<br>{}<br><br>Change in sentiment: {}%<br>Change in activity: {}%'.format(item[1], text, item[3], item[4])})
    
    df = pd.DataFrame(container, columns = ['time', 'user', 'text', 'dSent', 'dAct'])
    return df, annotations, shapes

@dApp.callback([Output('live-graph', 'children'),
               Output('since-update', 'children')],
              [Input('interval-component', 'n_intervals'),
               Input('dropdown_selector', 'value')])
def update_graph_live(n, ddValue):
    try:
        if ddValue is not None:
            timeSeries, sentiments = sqlSentimentSelect(ddValue)
            keywordDF = sqlKeywordSelect(ddValue)
            userDF, annotations, shapes = sqlUserSelect(ddValue)
            locationDF = sqlLocationSelect(ddValue)
        else:
            timeSeries = []
            sentiments = np.asarray([[0,0,0]])
    except IndexError as e:
        print(e)
        pass
    finally:
        red = '#FF3E30'
        blue = '#176BEF'
        green = '#17AF51'
        mapColorScale = [[0, red], [0.5, blue], [1.0, green]]
        if len(annotations) > 1:
            tableColor = tableColors(len(annotations))
        else:
            tableColor = ['lightcyan']
            
        sentimentChange = (sentiments[-1][1] - sentiments[-2][1]) - (sentiments[-1][0] - sentiments[-2][0])
        if sentimentChange > 0:
            sinceUpdateText = '+{}'.format(sentimentChange)
            sinceUpdateColor = green
        else:
            sinceUpdateText = '{}'.format(sentimentChange)
            sinceUpdateColor = red

    # Create the graph 
    graph = [
                html.Div([
                    html.Div([
                        dcc.Graph(
                            id='sentiment-line-graph',
                            figure={
                                'data': [
                                    go.Scatter(
                                        x=timeSeries,
                                        y=sentiments[0:,1],
                                        name="Positive",
                                        opacity=1,
                                        mode='lines',
                                        line=dict(width=1.25, color= green)
                                    ),
                                    go.Scatter(
                                        x=timeSeries,
                                        y=sentiments[0:,2],
                                        name="Neutral",
                                        opacity=1,
                                        mode='lines',
                                        line=dict(width=1.25, color= blue)
                                    ),
                                    go.Scatter(
                                        x=timeSeries,
                                        y=sentiments[0:,0],
                                        name="Negative",
                                        opacity=1,
                                        mode='lines',
                                        line=dict(width=1.25, color= red)
                                    )
                                ],
                                'layout':{
                                        'title': 'Sentiment over Time',
                                        'xaxis': {'rangeselector': {'buttons': [{'count': 1,
                                                                                 'label': '1h',
                                                                                 'step': 'hour',
                                                                                 'stepmode': 'backward'},
                                                                                {'count': 8,
                                                                                 'label': '8h',
                                                                                 'step': 'hour',
                                                                                 'stepmode': 'backward'},
                                                                                {'step': 'all',
                                                                                 'label': '12h'}]}},
                                        'yaxis': {'title': {'text': 'Number of Mentions'}},
                                        'shapes': shapes,
                                        'annotations': annotations,
                                        'margin':{'b': 40, 't': 30, 'r':20}
                                        }
                            }
                        )
                    ], style={'width': '70%', 'display': 'inline-block', 'padding': '0 0 0 20'}),
                    html.Div([
                            dcc.Graph(
                                id = 'sentiment-map',
                                figure={
                                    'data': [go.Choropleth(
                                        locations = locationDF['location'],
                                        z = locationDF['sentiment'],
                                        locationmode = 'USA-states',
                                        text = locationDF['text'],
                                        colorscale = mapColorScale,
                                        zmax = .7,
                                        zmin = .3,
#                                        colorbar_title = 'Sentiment',
                                        showscale=False
                                    )],
                                    'layout':{
                                            'geo': {'scope': 'usa'},
                                            'margin':{'l':5, 'r':5, 't': 60, 'b': 20},
                                            'title': 'Sentiment by State\n(past 30 minutes)'
                                    }
                                }
                            )
                    ], style={'width': '30%', 'display': 'inline-block', 'padding': '0 0 0 20'}),
                    html.Div([
                        dcc.Graph(
                            id = 'keyword-bar-graph',
                            figure={
                                'data': [
                                    go.Bar(
                                        y = keywordDF['word'],
                                        x = keywordDF['negative'],
                                        name = 'Negative',
                                        orientation = 'h',
                                        marker = dict(color  = red)
                                    ),
                                    go.Bar(
                                        y = keywordDF['word'],
                                        x = keywordDF['neutral'],
                                        name = 'Neutral',
                                        orientation = 'h',
                                        marker = dict(color  = blue)
                                    ),
                                    go.Bar(
                                        y = keywordDF['word'],
                                        x = keywordDF['positive'],
                                        name = 'Positive',
                                        orientation = 'h',
                                        marker = dict(color  = green)
                                    )
                                ],
                                'layout':{
                                    'xaxis': {'title': {'text': 'Times seen in past 30 minutes'}},
                                    'yaxis': {'title': {'text': 'Keyword'}},
                                    
                                    'height': 343,
                                    'margin':{'t': 25, 'b': 30, 'r': 10, 'l': 100},
                                    'font': {'size': 13},
                                    'hovermode':'closest',
                                    'barmode': 'stack'
                                }
                            }
                        )
                    ], style={'width': '50%', 'display': 'inline-block', 'padding': '0 0 0 100'}),
                    html.Div([
                        dcc.Graph(
                            id = 'twitter-table',
                            figure={
                                'data':[
                                    go.Table(
                                        columnwidth = [40, 60, 220, 55, 55],
                                        header = dict(values = ['Time', 'User', 'Text', 'Sentiment change', 'Activity change'],
                                                      fill_color = blue,
                                                      font_color = '#FFFFFF',
                                                      font_size = 17),
                                        cells = dict(values = [userDF['time'], userDF['user'], userDF['text'], userDF['dSent'], userDF['dAct']],
                                                     fill_color = tableColor,
                                                     suffix = ['', '', '', '%', '%'],
                                                     font_size = [12, 12, 12, 14, 14],
                                                     align = ['center', 'center', 'left', 'center', 'center'])
                                    )
                                ],
                                'layout':{
                                    'height': 343,
                                    'margin':{'t': 10, 'b': 10, 'l': 10}
                                } 
                            }
                        )
                    ], style={'width': '50%', 'display': 'inline-block', 'padding': '0 0 0 0'})
                ])
    ]
                                
    sinceUpdate = html.Div([
                    html.Div(html.H2(sinceUpdateText), style={'width': '15%', 'display': 'inline-block', 'color': sinceUpdateColor}),
                    html.Div(html.H3('Sentiment since last update'), style={'width': '80%', 'display': 'inline-block'})
                  ])
                                
                                
    return graph, sinceUpdate


topics = getRecentTopics()
dropdownLabelArray = []
for topic in topics:
    dropdownLabelArray.append({'label': topic, 'value': topic})
dropdown = [dcc.Dropdown(id = 'dropdown_selector', options = dropdownLabelArray, value = topics[0], 
                         placeholder = topics[0], persistence = True)]

print(topics)


dApp.layout = html.Div(children = [
        html.Div(html.H1('Real-time Twitter Analytics'), style={'width': '35%', 'display': 'inline-block', 'padding': '0 0 0 20'}),
        html.Div([html.Div(children = 'Topic:')], style={'width': '5%', 'display': 'inline-block', 'padding': '0 0 0 20', 'margin-right': -40, 'vertical-align': 13}),
        html.Div([html.Div(id = 'dropdown', children = dropdown)], style={'width': '15%', 'display': 'inline-block', 'padding': '0 0 0 20', 'margin-right': 150}),
        html.Div([html.Div(id = 'since-update')], style = {'width': '30%', 'display': 'inline-block'}),
        html.Div(id = 'live-graph', children = html.H2('Fetching graphs. This may take a few seconds!')),
        
        
        dcc.Interval(id = 'interval-component',
                     interval = 1*60000, #check every minute
                     n_intervals = 0
                     )
        ],
        style = {'padding': '20px'})

#TODO: selection resetting for some reason
@dApp.callback(Output('dropdown', 'children'),
              [Input('interval-component', 'n_intervals')])
def updateDropdown(n):
    topics = getRecentTopics()
    dropdownLabelArray = []
    for topic in topics:
        dropdownLabelArray.append({'label': topic, 'value': topic})
    dropdown = [dcc.Dropdown(id = 'dropdown_selector', options = dropdownLabelArray, value = topics[0], persistence = True)]
    return dropdown



if __name__ == '__main__':
    try:
        app.run(debug = True)
        pass
    except KeyboardInterrupt:
        print('\n\nKeyboard Interrupt')
