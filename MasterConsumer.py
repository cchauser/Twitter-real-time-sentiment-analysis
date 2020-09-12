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
import itertools
import MySQLdb as mysql

import pickle
from keras.models import load_model
from tensorflow.python.keras.preprocessing.sequence import pad_sequences
from kafka import KafkaConsumer
from nltk.corpus import stopwords
from nltk import word_tokenize



class masterConsumer(object):
    
    def __init__(self):
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
        print(self.consumer.bootstrap_connected())
        
        #This model is generalized sentiment analysis trained on sentiment140 from kaggle
        self.model = load_model('TwitSent.h5')
        with open('Toke_TwittSent.pkl', 'rb') as file:
            self.tokenizer = pickle.load(file)
            
        #TODO: Allow user to provide login at instantiation
        with open('mysqlKeys.txt') as f:
            keys = f.readlines()
        
        self.__mysqlUser = keys[0].replace('\n', '')
        self.__mysqlPass = keys[1].replace('\n', '')
        
        self.cnx = mysql.connect(user = self.__mysqlUser, 
                                 password = self.__mysqlPass, 
                                 host = '127.0.0.1',
                                 database = 'test')
        print('Connected to mySQL server')
        self.cursor = self.cnx.cursor()
        print('Cursor connected')


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
                sentimentChange = round((tweet[3] - tweet[2]) / (tweet[1]['positive'] - tweet[1]['negative']) * 100, 2)
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
#                print('Received packet from', message.headers[0][0])
                if message.headers[0][0] == 'userTrack':
                    if message.headers[1][0] in prevSentiment:
                        if message.headers[1][0] in userBuffer:
                            userBuffer[message.headers[1][0]].append([message.value, prevSentiment[message.headers[1][0]], 0, 0, 0])
                        else:
                            userBuffer[message.headers[1][0]] = [[message.value, prevSentiment[message.headers[1][0]], 0, 0, 0]]
                        userPacket = {'time': message.value['time'], 'user': message.value['user'], 
                                      'tweet': message.value['tweet'], 'deltasentiment': 0, 'deltaactivity': 0}
                        
                        self.sqlInsert(message.headers[1][0], 'user', userPacket)
                        

                    #We don't save tweets from target users that happen before we have a chance to get a baseline sentiment.
                    #This only occurs in the pollTimeSeconds window between consumer start up and the first batch being consumed
                else:
                    text = message.value['tweet']
                    text = self.removeStopWords(text)
                    #check if topic of stream is in the textbuffer already
                    if message.headers[0][0] in textBuffer:
                        textBuffer[message.headers[0][0]].append(text)
                    else:
                        self.createTables(message.headers[0][0])
                        textBuffer[message.headers[0][0]] = [text]
                        searchTerms[message.headers[0][0]] = message.value['terms'] + ['amp']
              
            currTime = int(time.time()) #Use int for database indexing purposes
    
            #Evaluate the buffer according to the pollTimeSeconds variable
            if currTime - previousPollTime >= pollTimeSeconds:
                ### SENTIMENT
                
                for topic in textBuffer:
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
                    vocab = word_freq.most_common(50+len(searchTerms)) # searchTerms will always be towards the top of the frequency distribution. This always returns top N non-search-terms
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
                        self.sqlInsert(topic, 'keyword', item)
                        
                    textBuffer[topic].clear()
                
                self.calculateSentimentChange(prevSentiment, userBuffer)
                
                print('processed {0} topics in {1:.2f} seconds\n'.format(len(textBuffer), time.time()-currTime))
                
                
                
                ### DON'T FORGET TO RESET YOUR BUFFER!
                searchTerms = {}
                previousPollTime = currTime
        
        
    #Topic will be the topic of the twitter stream the packet data was created from
    #TargetDB specifies whether it's keyword, user, or sentiment
    #Packet contains all the insert data
    def sqlInsert(self, topic, dbType, packet):
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
        
        self.cursor.execute(insertCommand, packet)
        self.cnx.commit()
    
    
    def sqlUpdate(self, topic, dbType, key, packet):
        table = '{}_{}'.format(topic, dbType)
        
        updateCommand = 'UPDATE {} SET '.format(table)
        for field in packet:
            updateCommand += '{0} = %({0})s, '.format(field)
        updateCommand = updateCommand[0:-2] + ' WHERE {0} = %({0})s'.format(key[0])
        
        packet[key[0]] = key[1] #Add key to packet so the cursor knows what to do with it
        
        self.cursor.execute(updateCommand, packet)
        self.cnx.commit()
    
    def createTables(self, topic):
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
        
        # Against best practices to have shared primary keys (in this case time was going to be the PK)
        # So make word_index the PK but for all intents and purposes time is the PK
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

        for tableName in TABLES:
            tableDesc = TABLES[tableName]
            try:
                self.cursor.execute(tableDesc)
                print('Created {}'.format(tableName))
            except Exception as err:
                print(err)
                
    def sqlDrop(self, user, password, topic):
        if user != self.__mysqlUser or password != self.__mysqlPass:
            return False
        else:
            t1 = '{}_sentiment'.format(topic)
            t2 = '{}_user'.format(topic)
            t3 = '{}_keyword'.format(topic)
            
            command =   '''DROP TABLE IF EXISTS
                            {}. {}. {} 
                        '''.format(t1,t2,t3)
            self.cursor.execute(command)
            self.cxn.commit()
            return True
        
    #TODO: Prune table entries that are too old to preserve disk space??

if __name__ == '__main__':   
    while True:
        try:
            mc = masterConsumer()
            mc.runConsumer()        
        except KeyboardInterrupt:
            print("\n\nKeyboard interrupt")
            break
        except Exception as e:
            print("\n\nException handler:")
            print(e)
            print('restarting')
        finally:
            mc.consumer.close()
            mc.cnx.close()

    print('Consumer closed successfully')