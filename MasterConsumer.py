#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  9 12:11:35 2020

@author: cullen
"""


import json
import time
import os
import string
import nltk
import itertools

import pickle
from keras.models import load_model
from datetime import datetime #i hate datetime
from tensorflow.python.keras.preprocessing.sequence import pad_sequences
from kafka import KafkaConsumer, KafkaProducer
from nltk.corpus import stopwords
from nltk import word_tokenize

print("Starting consumer")
consumer = KafkaConsumer(
    'TwitterStream',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: json.loads(x.decode('utf-8')),
     consumer_timeout_ms = 60000)
consumer.subscribe('TwitterStream')
print(consumer.subscription())
print(consumer.bootstrap_connected())

producer = KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_HOST', 'localhost:9092'),
                             value_serializer = lambda x: json.dumps(x).encode('utf-8'),
                             batch_size = 0)


#This model is generalized sentiment analysis trained on sentiment140 from kaggle
#I would recomment using this model and tokenizer for your modification to reduce processing time
model = load_model('TwitSent.h5')
with open('Toke_TwittSent.pkl', 'rb') as file:
    tokenizer = pickle.load(file)
pollTimeSeconds = 120 #two minutes to buffer

def sendData(packet, header):
    producer.send(topic = 'FrontEnd', value = packet, headers = [header])

def removeStopWords(text):
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

try:
    textBuffer = []
    previousPollTime = time.time()
    sentimentPacket = {'time': time.time(),'negative': 0, 'positive': 0, 'neutral': 0}
    for message in consumer:
        if message.topic == 'TwitterStream':
            if message.headers[0] == ('twitterTrack', b'1'):
                text = message.value['tweet']
                text = removeStopWords(text)
                searchTerms = message.value['terms'] + ['amp'] #TODO: retrain neural network without amp so i can filter it out in the stopword function
                textBuffer.append(text)
            elif message.headers[0] == ('twitterFollow', b'1'):
                userPacket = message.value
                #Use the previous two minute mark sentiment scores. The current one might already be reacting
                userPacket['negative'] = sentimentPacket['negative']
                userPacket['positive'] = sentimentPacket['positive']
                userPacket['neutral'] = sentimentPacket['neutral']
                
                t = datetime.fromtimestamp(userPacket['time']).strftime('%Y-%m-%d %H:%M:%S')
                userPacket['time'] = t
                print(userPacket)
                
                sendData(userPacket, ('twitterFollow', b'1'))
        ### IF ADDING MORE STREAMS TO BE PROCESSED ADD YOUR BUFFERS HERE FOLLOWING ABOVE EXAMPLE
        
        currTime = time.time()
        #Evaluate the buffer every 10 seconds
        if currTime - previousPollTime >= pollTimeSeconds:
            ### SENTIMENT
            
            #Pads and tokenizes tweets for input into the model. Padding value is 0 by default
            #Output should look like: [[0,0,0,0,...0,32,43,5,432],...[0,0,...0,4234,554,43]]
            #Tweet size limit is 280 chars, avg english word is 4.7 chars. Set padding to 70 to capture full tweet 90% of the time [citation needed]
            inputTweets = pad_sequences(tokenizer.texts_to_sequences(textBuffer), padding='pre', maxlen=70)
            p = model.predict(inputTweets)
            
            #Querying a numpy array for truth values returns an array of booleans which can be interpreted as 1's and 0's. Hence the sum call
            numPositive = int(sum(p > .6)[0]) #Transform to int because numpy prefers float-64 which kafka hates
            numNegative = int(sum(p < .4)[0])
            numNeutral = len(textBuffer) - (numPositive + numNegative)
            t = datetime.fromtimestamp(currTime).strftime('%Y-%m-%d %H:%M:%S')
            
            sentimentPacket = {'time': t,'negative': numNegative, 'positive': numPositive, 'neutral': numNeutral}
            print(len(inputTweets), sentimentPacket)

            sendData(sentimentPacket, ('twitterSentiment', b'1')) # b'1' denotes number 1 in byte typing. it's what kafka expects *shrug*
            
            
            ### WORD FREQUENCY
            allwords = [nltk.word_tokenize(comment) for comment in textBuffer]
            word_freq = nltk.FreqDist(itertools.chain(*allwords))
            #Vocab is list of tuples: [(word, frequency), ...]
            vocab = word_freq.most_common(50+len(searchTerms)) # searchTerms will always be towards the top of the frequency distribution. This always returns 20 non-search-terms
            i = 0
            while i < len(vocab):
                if vocab[i][0] in searchTerms:
                    vocab.pop(i)
                    continue
                i += 1
                
            keywordPacket = {'keywords': vocab}
            
            sendData(keywordPacket, ('keyWords', b'1'))
            
            
            
            ### HOW FUTURE USERS EXPAND ON THIS IS UP TO YOU
            ### I WOULD RECOMMEND CONCATENATING ALL OF THE BUFFERS INTO ONE AND THEN SEPERATING THEM OUT AFTER THE MODEL ANALYZES IT
            ### OR JUST COPY THE ABOVE EXAMPLE AND HAVE A SEPERATE PREDICT AND SEND. UP TO YOU
            
            
            ### DON'T FORGET TO RESET YOUR BUFFER!
            textBuffer = []
            previousPollTime = currTime
            
except KeyboardInterrupt:
    print("\n\nKeyboard interrupt")
    pass
except Exception as e:
    print("\n\nException handler:")
    print(e)
finally:
    consumer.close()
    print('Consumer closed succesfully')