#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  9 12:11:35 2020

@author: cullen
"""


import json
import time
import string
import nltk
import sys
import itertools
import MySQLdb as mysql

import pickle
from keras.models import load_model
from tensorflow.python.keras.preprocessing.sequence import pad_sequences
from kafka import KafkaConsumer
from nltk.corpus import stopwords
from nltk import word_tokenize

#You'll have to change your working directory if you run it on your own machine
wdir = '/home/cullen'


class masterConsumer(object):
    
    def __init__(self, DATABASE):
        print("Starting consumer")
        self.consumer = KafkaConsumer(
            'TwitterStream',
             bootstrap_servers=['localhost:9092'],
             auto_offset_reset='earliest',
             enable_auto_commit=True,
             group_id='my-group',
             value_deserializer=lambda x: json.loads(x.decode('utf-8')),
             consumer_timeout_ms = 60000)
        self.consumer.subscribe('TwitterStream')
        print(self.consumer.subscription())
        # print(self.consumer.bootstrap_connected())
        
        #This model is generalized sentiment analysis trained on sentiment140 from kaggle
        self.model = load_model('{}/models/TwitSent.h5'.format(wdir))
        with open('{}/models/Toke_TwittSent.pkl'.format(wdir), 'rb') as file:
            self.tokenizer = pickle.load(file)
            
        #TODO: Allow user to provide login at instantiation
        with open('{}/keys/mysqlKeys.txt'.format(wdir)) as f:
            keys = f.readlines()
        
        self.__mysqlUser = keys[0].replace('\n', '')
        self.__mysqlPass = keys[1].replace('\n', '')
        self.DATABASE = DATABASE


    def removeStopWords(self, text):
        tokenizedTweet = word_tokenize(text)
        stopWords = stopwords.words('english')
        newTweet = ''
        for word in tokenizedTweet:
            if word not in stopWords:
                if word in string.punctuation:
                    newTweet = newTweet.strip() + word + ' '
                else:
                    newTweet += word + ' '
        return newTweet.strip()
    
    
    #Calculates percentage change in activity and sentiment after a target user tweets
    #Then updates the SQL table
    def calculateSentimentChange(self, prevSent, userBuffer):
        for topic in prevSent:
            if topic not in userBuffer:
                continue
            for tweet in userBuffer[topic]:
                tweet[2] = prevSent[topic]['negative'] - tweet[1]['negative']
                tweet[3] = prevSent[topic]['positive'] - tweet[1]['positive']
                tweet[4] = prevSent[topic]['neutral'] - tweet[1]['neutral']
                
                #Percentage of change from the original measurement
                sentimentChange = round(((prevSent[topic]['positive'] - prevSent[topic]['negative']) - (tweet[1]['positive'] - tweet[1]['negative'])) / abs(tweet[1]['positive'] - tweet[1]['negative']) * 100, 2)
                activityChange = round((tweet[2] + tweet[3] + tweet[4]) / (tweet[1]['negative'] + tweet[1]['positive'] + tweet[1]['neutral']) * 100, 2)
                
                #Update the table
                updatePacket = {'deltasentiment': sentimentChange, 'deltaactivity': activityChange}
                key = ['time', tweet[0]['time']]
                self.sqlUpdate(topic, 'user', key, updatePacket)
                
                #Stop updating the sentiment and activity change after a set amount of time.
                if int(time.time()) - tweet[0]['time'] > 420:
                    userBuffer[topic].remove(tweet)
                
    
    def runConsumer(self):
        pollTimeSeconds = 180 #buffer for 3 minutes. any lower and you risk the kafka server producing latency
        textBuffer = {}
        searchTerms = {}
        userBuffer = {}
        prevSentiment = {}
        previousPollTime = int(time.time())
        for message in self.consumer:
            #Processes ~1700 per min. Estimate pollTimeSeconds based on this info
            if message.topic == 'TwitterStream':
                # print('Received packet from', message.value['topic'][0])
                if message.value['topic'][0] == 'userTrack':
                    if message.value['topic'][1] in prevSentiment:
                        if message.value['topic'][1] in userBuffer:
                            userBuffer[message.value['topic'][1]].append([message.value, prevSentiment[message.value['topic'][1]], 0, 0, 0])
                        else:
                            userBuffer[message.value['topic'][1]] = [[message.value, prevSentiment[message.value['topic'][1]], 0, 0, 0]]
                        userPacket = {'time': message.value['time'], 'user': message.value['user'], 
                                      'tweet': message.value['tweet'], 'deltasentiment': 0, 'deltaactivity': 0}
                        self.sqlInsert(message.value['topic'][1], 'user', userPacket)
                        

                    #We don't save tweets from target users that happen before we have a chance to get a baseline sentiment.
                    #This only occurs in the pollTimeSeconds window between consumer start up and the first batch being consumed
                else:
                    text = message.value['tweet']
                    text = self.removeStopWords(text)
                    #check if topic of stream is in the textbuffer already
                    if message.value['topic'][0] in textBuffer:
                        textBuffer[message.value['topic'][0]].append(text)
                    else:
                        self.createTables(message.value['topic'][0])
                        textBuffer[message.value['topic'][0]] = [text]
                        searchTerms[message.value['topic'][0]] = message.value['terms'] + ['amp'] #TODO: if search terms change a restart is required to reflect changes
              
            currTime = int(time.time()) #Use int for database indexing purposes
    
            #Evaluate the buffer according to the pollTimeSeconds variable
            if currTime - previousPollTime >= pollTimeSeconds:
                ### SENTIMENT
                
                for topic in textBuffer:
                    if len(textBuffer[topic]) == 0:
                        continue
                    #TODO: Multiprocess/thread this with mutexes. When there get to be a lot of streams it WILL fall behind
                    
                    
                    #Pads and tokenizes tweets for input into the model. Padding value is 0 by default
                    #Output should look like: [[0,0,0,0,...0,32,43,5,432],...[0,0,...0,4234,554,43]]
                    #Use pre-padding because LSTMs are somewhat biased to the end of an input.
                    #Tweet size limit is 280 chars, avg english word is 4.7 chars. Set padding to 70 to capture full tweet 90% of the time [citation needed]
                    inputTweets = pad_sequences(self.tokenizer.texts_to_sequences(textBuffer[topic]), padding='pre', maxlen=70)
                    p = self.model.predict(inputTweets)
                    
                    #Querying a numpy array for truth values returns an array of booleans which can be interpreted as 1's and 0's. Hence the sum call
                    numPositive = int(sum(p > .6)[0]) #Transform to int for data consumption purposes
                    numNegative = int(sum(p < .4)[0])
                    numNeutral = len(textBuffer[topic]) - (numPositive + numNegative)
                    
                    sentimentPacket = {'time': currTime,'negative': numNegative, 'positive': numPositive, 'neutral': numNeutral}
                    prevSentiment[topic] = sentimentPacket
                    print(topic, len(inputTweets), sentimentPacket)
                    self.sqlInsert(topic, 'sentiment', sentimentPacket)
                    
                    
                    ### WORD FREQUENCY
                    allwords = [nltk.word_tokenize(comment) for comment in textBuffer[topic]]
                    word_freq = nltk.FreqDist(itertools.chain(*allwords))
                    #Vocab is list of tuples: [(word, frequency), ...]
                    vocab = word_freq.most_common(50+len(searchTerms[topic])) # searchTerms will always be towards the top of the frequency distribution. This always returns top N non-search-terms
                    i = 0
                    keywordPacket = []
                    while i < len(vocab):
                        if vocab[i][0] in searchTerms[topic] or vocab[i][0] == 'amp':
                            vocab.pop(i)
                            continue #do NOT iterate i after a pop
                        keywordPacket.append({'time': currTime,
                                              'word': vocab[i][0],
                                              'times_seen': vocab[i][1]})
                        i += 1
                        
                    #TODO: Why use many insert commands when one do trick?
                    for item in keywordPacket:
                        try:
                            self.sqlInsert(topic, 'keyword', item)
                        except:
                            continue
                        
                    textBuffer[topic].clear()
                
                self.calculateSentimentChange(prevSentiment, userBuffer)
                
                print('processed {0} topics in {1:.2f} seconds\n'.format(len(textBuffer), time.time()-currTime))
                
                
                
                ### DON'T FORGET TO RESET YOUR BUFFER!
                previousPollTime = currTime
        
        
    #Topic will be the topic of the twitter stream the packet data was created from
    #TargetDB specifies whether it's keyword, user, or sentiment
    #Packet contains all the insert data
    def sqlInsert(self, topic, dbType, packet):
        cnx = mysql.connect(user = self.__mysqlUser, 
                            password = self.__mysqlPass, 
                            host = '127.0.0.1',
                            database = self.DATABASE)
        cursor = cnx.cursor()
        #Table name schema topic_type (eg. nasa_sentiment)
        table = '{}_{}'.format(topic, dbType)
        
        insertCommand = 'INSERT INTO {} ('.format(table)
        for field in packet:
            insertCommand += '{}, '.format(field)
        insertCommand = insertCommand[0:-2] + ') VALUES (%('
        for field in packet:
            insertCommand += '{})s, %('.format(field)
        insertCommand = insertCommand[0:-4] + ')'
        #End result:
        #'INSERT INTO topic_dbType (field1, ..., fieldn) VALUES (%(field1)s, ..., %(fieldn)s)'
        
        cursor.execute(insertCommand, packet)
        cnx.commit()
        cnx.close()
    
    
    def sqlUpdate(self, topic, dbType, key, packet):
        cnx = mysql.connect(user = self.__mysqlUser, 
                            password = self.__mysqlPass, 
                            host = '127.0.0.1',
                            database = self.DATABASE)
        cursor = cnx.cursor()
        table = '{}_{}'.format(topic, dbType)
        
        updateCommand = 'UPDATE {} SET '.format(table)
        for field in packet:
            updateCommand += '{0} = %({0})s, '.format(field)
        updateCommand = updateCommand[0:-2] + ' WHERE {0} = %({0})s'.format(key[0])
        
        packet[key[0]] = key[1] #Add key to packet so the cursor knows what to do with it
        
        cursor.execute(updateCommand, packet)
        cnx.commit()
        cnx.close()
    
    def createTables(self, topic):
        cnx = mysql.connect(user = self.__mysqlUser, 
                            password = self.__mysqlPass, 
                            host = '127.0.0.1',
                            database = self.DATABASE)
        cursor = cnx.cursor()
        
        sentTableName = '{}_sentiment'.format(topic)
        userTableName = '{}_user'.format(topic)
        keywordTableName = '{}_keyword'.format(topic)
        
        TABLES = {}
        TABLES[sentTableName] = (
                '''
                CREATE TABLE {} (
                time int(11) NOT NULL, 
                negative int(11) NOT NULL, 
                positive int(11) NOT NULL, 
                neutral int(11) NOT NULL, 
                PRIMARY KEY (time) ) 
                ENGINE = InnoDB'''.format(sentTableName)
                )
        
        TABLES[userTableName] = (
                '''
                CREATE TABLE {} (
                time int(11) NOT NULL,
                user varchar(15) NOT NULL,
                tweet varchar(280) NOT NULL,
                deltasentiment int(11) NOT NULL,
                deltaactivity int(11) NOT NULL,
                PRIMARY KEY (time) ) 
                ENGINE = InnoDB'''.format(userTableName)
                )
        
        # It is against best practices to have shared primary keys (in this case time was going to be the PK)
        # So make word_index the PK but for all intents and purposes time is the filed being used to find records
        TABLES[keywordTableName] = (
                '''
                CREATE TABLE {} (
                word_index int(11) NOT NULL AUTO_INCREMENT,
                time int(11) NOT NULL,
                word varchar(20) NOT NULL,
                times_seen int(11) NOT NULL,
                PRIMARY KEY (word_index) ) 
                ENGINE = InnoDB'''.format(keywordTableName)
                )

        #TODO: Touch table to update the LAST_UPDATE field. Allows frontendDash to start without error
        for tableName in TABLES:
            tableDesc = TABLES[tableName]
            try:
                cursor.execute(tableDesc)
                print('Created {}'.format(tableName))
            except Exception as err:
                print(err)
        cnx.close()
                
        
    #TODO: Prune table entries that are too old to preserve disk space??

if __name__ == '__main__':
    while True:
        try:
            mc = masterConsumer('production')
            mc.runConsumer()        
        except KeyboardInterrupt:
            print("\n\nKeyboard interrupt")
            break
        except Exception as e:
            print("\n\nException handler:")
            print(e)
            print('On line {}'.format(sys.exc_info()[-1].tb_lineno))
            print('restarting')
        finally:
            mc.consumer.close()
            
    print('Consumer closed successfully')