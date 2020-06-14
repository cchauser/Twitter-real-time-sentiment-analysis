#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 11 13:31:14 2020

@author: cullen
"""

import dash
import json
import time
import os
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go

from kafka import KafkaConsumer
from dash.dependencies import Input, Output
from datetime import datetime


try:
    prefix = datetime.fromtimestamp(time.time()).strftime('%m%d')
    sentimentDF = pd.read_csv('{0}/{0}-sentimentDF.csv'.format(prefix))
    keywordDF = pd.read_csv('{0}/{0}-keywordDF.csv'.format(prefix))
    userDF = pd.read_csv('{0}/{0}-userDF.csv'.format(prefix))
except Exception as e:
    print(e)
    sentimentDF = pd.DataFrame(columns = ['Time', 'Number of Tweets', 'Sentiment'])
    
    keywordDF = pd.DataFrame({'Word': ['placeholder'], 'Freq': [0]})
    
    userDF = pd.DataFrame(columns = ['Time', 'imageURL', 'User', 'Text', 'neg', 'pos', 'neu', 'dAttitude', 'dActivity'])

consumer = KafkaConsumer(
    'FrontEnd',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: json.loads(x.decode('utf-8')),
     consumer_timeout_ms = 60000)



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.title = 'Real-Time Analytics'

server = app.server

app.layout = html.Div(children = [
        html.Div(html.H1('Analytics'), style={'width': '25%', 'display': 'inline-block', 'padding': '0 0 0 20'}),
        html.Div([html.Button('Save Data', id='save-button', n_clicks=0),
                  html.Div(id='save_success', children = 'Click this button to save the data')], style={'width': '50%', 'display': 'inline-block', 'padding': '0 0 0 20'}),
        html.Div(id = 'live-graph'),
        
        
        dcc.Interval(id = 'interval-component',
                     interval = 1*60000, 
                     n_intervals = 0
                     )
        ],
        style = {'padding': '20px'})
        
#God forgive me for what i'm about to do
def updateUserDF():
    global sentimentDF, userDF

    lastPositiveScore = sentimentDF['Number of Tweets'].at[len(sentimentDF)-2]
    lastNegativeScore = sentimentDF['Number of Tweets'].at[len(sentimentDF)-3]
    lastNeutralScore = sentimentDF['Number of Tweets'].at[len(sentimentDF)-1]
    
    for i in range(len(userDF)):
        userT = datetime.strptime(userDF['Time'].at[i], '%Y-%m-%d %H:%M:%S')
        deltaTime = datetime.now() - userT
        minutesPassed = int(deltaTime.total_seconds()) // 60
        
        if minutesPassed <= 5:
            userPos = userDF['pos'].at[i]
            userNeg = userDF['neg'].at[i]
            userNeu = userDF['neu'].at[i]
            
            attitudeChange = round(((lastPositiveScore - userPos) - (lastNegativeScore - userNeg)) / (userPos - userNeg) * 100, 2)
            activityChange = round(((lastPositiveScore + lastNegativeScore + lastNeutralScore) - (userPos + userNeg + userNeu)) / (userPos + userNeg + userNeu) * 100, 2)
            
            userDF['dAttitude'].at[i] = attitudeChange
            userDF['dActivity'].at[i] = activityChange
    

# d is date offset. ex: d = -1 saves data using yesterday's date
def saveCSV(d = 0):
    global sentimentDF, keywordDF, userDF
    
    date = datetime.now()
    
    #TODO: Format correctly. ATM if month is 1 character long it doesn't prepend a 0. Format should be 0611. Currently 611
    prefix = '{0}{1}'.format(date.month, date.day+d)
    
    try:
        os.mkdir(prefix)
    except:
        pass
    
    sentimentDF.to_csv('{0}/{0}-sentimentDF.csv'.format(prefix), index=False)
    keywordDF.to_csv('{0}/{0}-keywordDF.csv'.format(prefix), index=False)
    userDF.to_csv('{0}/{0}-userDF.csv'.format(prefix), index=False)
    
def resetDF():
    global sentimentDF, keywordDF, userDF
    
    sentimentDF = pd.DataFrame(columns = ['Time', 'Number of Tweets', 'Sentiment'])
    keywordDF = pd.DataFrame({'Word': ['placeholder'], 'Freq': [0]})
    userDF = pd.DataFrame(columns = ['Time', 'imageURL', 'User', 'Text', 'neg', 'pos', 'neu', 'dAttitude', 'dActivity'])


def tableColors():
    global userDF
    
    oddColor = 'white'
    evenColor = 'lightcyan'
    
    tableColor = [oddColor, evenColor] * round(len(userDF)/2)
    #Cut off at the length of userDF
    #Because the table displays the results with the most recent at the top we reverse the order for
    #the colors so that all of the entries retain the same colors and the newest gets the next color
    tableColor = tableColor[:len(userDF)][::-1]
    return [tableColor * 5]


@app.callback(Output('save_success', 'children'),
              [Input('save-button', 'n_clicks')])
def button_save_csv(n_clicks):
    global sentimentDF, keywordDF, userDF
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
    


@app.callback(Output('live-graph', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_graph_live(n):
    global sentimentDF, keywordDF, userDF
    
    currTime = datetime.now()
    #Only occurs at midnight
    #Can possibly skip over this if there is a large spike in data to be processed by the consumer
    #near midnight thus stalling the consumer and taking up processor cycles for a crucial
    #few seconds which pushes the time the front end processes new data beyond 00:00 
    #This bug shouldn't ever occur. But I know it will eventually. You've been warned
    if currTime.hour + currTime.minute == 0:
        saveCSV(d = -1) #Save before resetting
        resetDF() #Reset the data at midnight local time
    
    new_data = consumer.poll()
    try:
        new_data = list(new_data.values())[0] #Gives a list of new packets
        
        for _packet in new_data:
            if _packet.headers[0] == ('twitterSentiment', b'1'):
                packet = _packet.value
                t = packet['time']
                numNeg = packet['negative']
                numPos = packet['positive']
                numNeu = packet['neutral']
                dfNeg = pd.DataFrame({'Time': t, 'Number of Tweets': numNeg, 'Sentiment': 0}, index = [0])
                sentimentDF = sentimentDF.append(dfNeg)
                dfPos = pd.DataFrame({'Time': t, 'Number of Tweets': numPos, 'Sentiment': 2}, index = [0])
                sentimentDF = sentimentDF.append(dfPos)
                dfNeu = pd.DataFrame({'Time': t, 'Number of Tweets': numNeu, 'Sentiment': 1}, index = [0])
                sentimentDF = sentimentDF.append(dfNeu)
            elif _packet.headers[0] == ('keyWords', b'1'):
                packet = _packet.value['keywords']
                wordList = list(keywordDF['Word'])
                for item in packet:
                    if item[0] not in wordList:
                        newWordDF = pd.DataFrame({'Word': item[0], 'Freq': item[1]}, index = [0])
                        keywordDF = keywordDF.append(newWordDF)
                    else:
                        keywordDF['Freq'][keywordDF['Word'] == item[0]] = keywordDF['Freq'][keywordDF['Word'] == item[0]] + item[1] #Using += results in a warning being printed
            elif _packet.headers[0] == ('twitterFollow', b'1'):
                packet = _packet.value
                newDF = pd.DataFrame({'Time': packet['time'], 'imageURL': packet['image_url'], 'User': packet['user'],
                                      'Text': packet['tweet'], 'neg': packet['negative'], 'pos': packet['positive'],
                                      'neu': packet['neutral'], 'dAttitude': 0.0, 'dActivity': 0.0}, index = [0])
                userDF = userDF.append(newDF)

    except IndexError as e:
        print(e)
        pass
    finally:
        #After appending the dataframes, do this to reset the indices or they'll all be zero
        sentimentDF.reset_index(inplace = True, drop = True)
        keywordDF.reset_index(inplace = True, drop = True)
        userDF.reset_index(inplace = True, drop = True)
        
        time_series = sentimentDF['Time'][sentimentDF['Sentiment']==0].reset_index(drop=True)
        wordFreq = keywordDF.nlargest(15, 'Freq')
        
        if len(userDF) > 0 and len(sentimentDF) > 0:
            updateUserDF()
            
        tableColor = tableColors()

    # Create the graph 
    children = [
                html.Div([
                    html.Div([
                        dcc.Graph(
                            id='sentiment-line-graph',
                            figure={
                                'data': [
                                    #TODO: Disconnect line graph if there are gaps. DIFFICULT!
                                    #TODO: Spline shape?
                                    go.Scatter(
                                        x=time_series,
                                        y=sentimentDF['Number of Tweets'][sentimentDF['Sentiment']==1].reset_index(drop=True),
                                        name="Neutral",
                                        opacity=1,
                                        mode='lines',
                                        line=dict(width=1, color='rgb(50, 50, 255)')
                                    ),
                                    go.Scatter(
                                        x=time_series,
                                        y=sentimentDF['Number of Tweets'][sentimentDF['Sentiment']==0].reset_index(drop=True),
                                        name="Negative",
                                        opacity=1,
                                        mode='lines',
                                        line=dict(width=1, color='rgb(255, 50, 50)')
                                    ),
                                    go.Scatter(
                                        x=time_series,
                                        y=sentimentDF['Number of Tweets'][sentimentDF['Sentiment']==2].reset_index(drop=True),
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
                                                                                {'step': 'all'}]}},
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
                                        x = wordFreq['Freq'][::-1],
                                        y = wordFreq['Word'][::-1],
                                        orientation = 'h',
                                        marker_color = 'lightskyblue'
                                    )
                                ],
                                'layout':{
                                    'xaxis': {'title': {'text': 'Times seen'}},
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
                                        cells = dict(values = [userDF['Time'][::-1], userDF['User'][::-1], userDF['Text'][::-1], userDF['dAttitude'][::-1], userDF['dActivity'][::-1]],
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
    return children


try:
    app.run_server(debug = True)
except KeyboardInterrupt:
    print('\n\nKeyboard Interrupt')
finally:
    #Make sure consumer closes successfully or it locks up the kafka server until you restart it
    consumer.close()
    print('Consumer closed successfully')
    
    #Save csvs before closing/restarting.
#    saveCSV()
