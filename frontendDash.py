#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 11 13:31:14 2020

@author: cullen
"""

import dash
import time
import os
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import numpy as np
import plotly.graph_objs as go
import MySQLdb as mysql

from dash.dependencies import Input, Output
from datetime import datetime, timedelta

DATABASE = 'test'


with open('mysqlKeys.txt') as f:
    keys = f.readlines()

mysqlUser = keys[0].replace('\n', '')
mysqlPass = keys[1].replace('\n', '')


print('Connected to mySQL server')


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets, suppress_callback_exceptions=True)

app.title = 'Real-Time Analytics'

server = app.server


# d is date offset. ex: d = -1 saves data using yesterday's date
def saveCSV(d = 0):
    
    date = datetime.now()
    date -= timedelta(days = d)
    
    prefix = date.strftime('%m%d')
    
    try:
        os.mkdir(prefix)
    except:
        pass
    
    sentimentDF.to_csv('{0}/{0}-sentimentDF.csv'.format(prefix), index=False)
    keywordDF.to_csv('{0}/{0}-keywordDF.csv'.format(prefix), index=False)
    userDF.to_csv('{0}/{0}-userDF.csv'.format(prefix), index=False)
    

def tableColors(n):
    
    oddColor = 'lightcyan'
    evenColor = 'white'
    
    tableColor = [oddColor, evenColor] * round(n/2)
    #Cut off at the length of userDF
    #Because the table displays the results with the most recent at the top we reverse the order for
    #the colors so that all of the entries retain the same colors and the newest gets the next color
    tableColor = tableColor[:n][::-1]
    return [tableColor * 5]

@app.callback(Output('save_success', 'children'),
              [Input('save-button', 'n_clicks')])
def button_save_csv(n_clicks):
    if n_clicks > 0:
    
        prefix = datetime.fromtimestamp(time.time()).strftime('%m%d')
        try:
            os.mkdir(prefix)
        except:
            pass
        
        sentimentDF.to_csv('{0}/{0}-sentimentDF.csv'.format(prefix), index=False)
        keywordDF.to_csv('{0}/{0}-keywordDF.csv'.format(prefix), index=False)
        userDF.to_csv('{0}/{0}-userDF.csv'.format(prefix), index=False)
        return 'Save Successful'
    else:
        return 'Click this button to save the data'
    
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
        if (datetime.now() - entry[1]).seconds / 3600 < 12:
            topics.append(entry[0].split('_')[0])
    cnx.close()
    return topics

def sqlSentimentSelect(topic):
    cnx = mysql.connect(user = mysqlUser, 
                         password = mysqlPass, 
                         host = '127.0.0.1',
                         database = DATABASE)
    cursor = cnx.cursor()
    dayAgoSeconds = int(time.time()) - 86400
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
                    SELECT word, SUM(times_seen) as n
                    FROM {0}_keyword
                    WHERE time > {1}
                    GROUP BY word
                    ORDER BY n  
                  '''.format(topic, minTimeSelection)
    
    cursor.execute(selectQuery)
    wordArray = []
    countArray = []
    for item in cursor:
        wordArray.append(item[0])
        countArray.append(int(item[1]))
    cnx.close()
    return wordArray[:15], countArray[:15]
    
def sqlUserSelect(topic):
    cnx = mysql.connect(user = mysqlUser, 
                         password = mysqlPass, 
                         host = '127.0.0.1',
                         database = DATABASE)
    cursor = cnx.cursor()
    
    dayAgoSeconds = int(time.time()) - 86400
    selectQuery = '''
                  SELECT time, user, tweet, deltasentiment, deltaactivity
                  FROM {0}_user
                  WHERE time > {1}
                  ORDER BY time DESC
                  '''.format(topic, dayAgoSeconds)
    
    cursor.execute(selectQuery)
    #TODO: Better way to do this
    timeArray = []
    userArray = []
    tweetArray = []
    dSentArray = []
    dActArray = []
    for item in cursor:
        t = datetime.fromtimestamp(item[0]).strftime('%Y-%m-%d %H:%M:%S')
        timeArray.append(t)
        userArray.append(item[1])
        tweetArray.append(item[2])
        dSentArray.append(item[3])
        dActArray.append(item[4])
    
    cnx.close()
    return timeArray, userArray, tweetArray, dSentArray, dActArray

@app.callback([Output('live-graph', 'children'),
               Output('since-update', 'children')],
              [Input('interval-component', 'n_intervals'),
               Input('dropdown_selector', 'value')])
def update_graph_live(n, ddValue):
    try:
        if ddValue != None:
            timeSeries, sentiments = sqlSentimentSelect(ddValue)
            wordArray, countArray = sqlKeywordSelect(ddValue)
            timeArray, userArray, tweetArray, dSentArray, dActArray = sqlUserSelect(ddValue)
        else:
            timeSeries = []
            sentiments = np.asarray([[0,0,0]])
    except IndexError as e:
        print(e)
        pass
    finally:
        if len(timeArray) > 1:
            tableColor = tableColors(len(timeArray))
        else:
            tableColor = ['lightcyan']
            
        sentimentChange = (sentiments[-1][1] - sentiments[-2][1]) - (sentiments[-1][0] - sentiments[-2][0])
        if sentimentChange > 0:
            sinceUpdateText = '+{}'.format(sentimentChange)
            sinceUpdateColor = 'green'
        else:
            sinceUpdateText = '{}'.format(sentimentChange)
            sinceUpdateColor = 'red'

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
                                        y=sentiments[0:,2],
                                        name="Neutral",
                                        opacity=1,
                                        mode='lines',
                                        line=dict(width=1, color='rgb(50, 50, 255)')
                                    ),
                                    go.Scatter(
                                        x=timeSeries,
                                        y=sentiments[0:,0],
                                        name="Negative",
                                        opacity=1,
                                        mode='lines',
                                        line=dict(width=1, color='rgb(255, 50, 50)')
                                    ),
                                    go.Scatter(
                                        x=timeSeries,
                                        y=sentiments[0:,1],
                                        name="Positive",
                                        opacity=1,
                                        mode='lines',
                                        line=dict(width=1, color='rgb(50, 255, 50)')
                                    )
                                ],
                                'layout':{
                                        'xaxis': {'rangeselector': {'buttons': [{'count': 1,
                                                                                 'label': '1h',
                                                                                 'step': 'hour',
                                                                                 'stepmode': 'backward'},
                                                                                {'count': 8,
                                                                                 'label': '8h',
                                                                                 'step': 'hour',
                                                                                 'stepmode': 'backward'},
                                                                                {'count': 12,
                                                                                 'label': '12h',
                                                                                 'step': 'hour',
                                                                                 'stepmode': 'backward'},
                                                                                {'step': 'all',
                                                                                 'label': '24h'}]}},
                                        'yaxis': {'title': {'text': 'Number of Mentions'}},
                                        'margin':{'b': 40, 't': 30}
                                        }
                            }
                        )
                    ], style={'width': '100%', 'display': 'inline-block', 'padding': '0 0 0 20'}),
                    html.Div([
                        dcc.Graph(
                            id = 'keyword-bar-graph',
                            figure={
                                'data': [
                                    go.Bar(
                                        x = countArray,
                                        y = wordArray,
                                        orientation = 'h',
                                        marker_color = 'lightskyblue'
                                    )
                                ],
                                'layout':{
                                    'xaxis': {'title': {'text': 'Times seen in past 30 minutes'}},
                                    'yaxis': {'title': {'text': 'Keyword'}},
                                    'height': 343,
                                    'margin':{'t': 10, 'b': 30, 'r': 10},
                                    'font': {'size': 12},
                                    'hovermode':'closest'
                                }
                            }
                        )
                    ], style={'width': '50%', 'display': 'inline-block', 'padding': '0 0 0 20'}),
                    html.Div([
                        dcc.Graph(
                            id = 'twitter-table',
                            figure={
                                'data':[
                                    go.Table(
                                        columnwidth = [40, 60, 220, 55, 55],
                                        header = dict(values = ['Time', 'User', 'Text', 'Sentiment change', 'Activity change'],
                                                      fill_color = 'lightskyblue',
                                                      font_size = 17),
                                        cells = dict(values = [timeArray, userArray, tweetArray, dSentArray, dActArray],
                                                     fill_color = tableColor,
                                                     suffix = ['', '', '', '%', '%'],
                                                     font_size = [12, 12, 12, 14, 14],
                                                     align = ['center', 'center', 'left', 'center', 'center'])
                                    )
                                ],
                                #TODO: Pretty layout
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


app.layout = html.Div(children = [
        html.Div(html.H1('Real-time Twitter Analytics'), style={'width': '35%', 'display': 'inline-block', 'padding': '0 0 0 20'}),
        html.Div([html.Div(children = 'Topic:')], style={'width': '5%', 'display': 'inline-block', 'padding': '0 0 0 20', 'margin-right': -40, 'vertical-align': 13}),
        html.Div([html.Div(id = 'dropdown', children = dropdown)], style={'width': '15%', 'display': 'inline-block', 'padding': '0 0 0 20', 'margin-right': 150}),
        html.Div([html.Div(id = 'since-update')], style = {'width': '30%', 'display': 'inline-block'}),
        html.Div(id = 'live-graph'),
        
        
        dcc.Interval(id = 'interval-component',
                     interval = 1*60000, #check every minute
                     n_intervals = 0
                     )
        ],
        style = {'padding': '20px'})

#TODO: selection resetting for some reason
@app.callback(Output('dropdown', 'children'),
              [Input('interval-component', 'n_intervals')])
def updateDropdown(n):
    topics = getRecentTopics()
    dropdownLabelArray = []
    for topic in topics:
        dropdownLabelArray.append({'label': topic, 'value': topic})
    dropdown = [dcc.Dropdown(id = 'dropdown_selector', options = dropdownLabelArray, persistence = True)]
    return dropdown




try:
    app.run_server(debug = True)
except KeyboardInterrupt:
    print('\n\nKeyboard Interrupt')
finally:
    #Make sure consumer closes successfully or it locks up the kafka server until you restart it
    print('Consumer closed successfully')
    
    #Save csvs before closing/restarting.
#    saveCSV()
