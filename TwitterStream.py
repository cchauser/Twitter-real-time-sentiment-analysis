# -*- coding: utf-8 -*-
"""
Created on Thu May  7 09:57:30 2020

@author: Cullen
"""

from tweepy.auth import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import KafkaProducer
from time import time, sleep
from nltk import word_tokenize
from nltk.corpus import stopwords
import multiprocessing
import string
import json
import os
import re

#You'll have to change your working directory if you run it on your own machine
wdir = '/home/cullen'

with open('{}/keys/twitterkeys.txt'.format(wdir)) as f:
    keys = f.readlines()

consumer_key = keys[0].replace('\n', '')
consumer_secret= keys[1].replace('\n', '')
access_token= keys[2].replace('\n', '')
access_secret= keys[3].replace('\n', '')

httpRegex = "@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+"


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
 

# we create this class that inherits from the StreamListener in tweepy StreamListener
class TweetsListener(StreamListener):
    
    def __init__(self, keywordArg, userArg, filterwords):
        self.userArg = userArg
        self.keywordArg = keywordArg + filterwords
        self.producer = KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_HOST', 'localhost:9092'),
                             value_serializer = lambda x: json.dumps(x).encode('utf-8'),
                             batch_size = 0)
        self.topic = keywordArg[0]
        
        
    # we override the on_data() function in StreamListener
    def on_data(self, data):
        try:
            message = json.loads(data)
            if 'retweeted_status' in message:
                return True
            else:
                #Twitter clips off long tweets and flags as truncated then moves them to an extended tweet section in the json
                if message['truncated']:
                    tweet = message['extended_tweet']['full_text']
                else:
                    tweet = message['text']
                    
                    
                if message['user']['id_str'] in self.userArg:
                    tweet = tweet.encode('ASCII', 'ignore').decode() #Removes emojis
                    while '  ' in tweet:
                        tweet = tweet.replace('  ', ' ')
                    
                    print(tweet, message['user']['screen_name'])
                    packet = {'topic': ['userTrack', self.topic], 'time': int(time()), 'user': message['user']['screen_name'], 'tweet': tweet}
                    self.producer.send('TwitterStream', value=packet)
                else:
                    tweet = tweet.lower()
                    tweet = tweet.replace('\n\n', ' ')
                    tweet = tweet.replace('\n', ' ')
                    tweet = re.sub(httpRegex, ' ', str(tweet).strip()).strip()
                    
                    #May have to perform multiple times if there are triple spaces
                    while '  ' in tweet:
                        tweet = tweet.replace('  ', ' ')
                        
                    tweet = removeStopWords(tweet)
                        
                    if len(tweet) < 3:
                        return True
                    
                    
                    # print(self.topic)
                    packet = {'topic': [self.topic], 'tweet': tweet, 'terms': self.keywordArg}
                    self.producer.send('TwitterStream', value=packet)
            return True
        except Exception as e:
            pass
            # print("Error on_data: %s" % str(e))
        return True

    def if_error(self, status):
        print(status)
        return True
    
    def on_exception(self, exception):
        print(exception)
        return True


# Filter words are common words associated with what you're searching for but are so common they're going to skew
# the keyword graph and are also too common to search for individually. For example if you want results on Joe Biden
# you'll have the stream listen for 'biden' but not joe since that's too common, then filterwords will include 'joe'
# 'joe' is going to appear with 'biden' often.
def startStream(keywords, users, filterwords = []):
    print('Starting stream using', keywords)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    print('Authorization successful: ', auth.oauth.verify)
    twitter_stream = Stream(auth, TweetsListener(keywords, users, filterwords))
    while True:
        try:
            twitter_stream.filter(languages = ['en'], track=keywords, follow = users) # start the stream
        except KeyboardInterrupt:
            print('Keyboard Interrupt')
            return True
        except Exception as e:
            print(e)
            print("IM HERE")
            continue


if __name__ == "__main__":
    # p = multiprocessing.Process(target = startStream, args=(['biden', 'kamala', 'joebiden'], ['939091'], ['joe']))
    # p.start()
    # sleep(2)
    p = multiprocessing.Process(target = startStream, args=(['heat', 'jimmy butler', 'herro'], ['11026952'], ['tyler', 'miami']))
    p.start()
    sleep(2)
    # c = multiprocessing.Process(target = startStream, args=(['trump', 'pence', 'realdonaldtrump'], ['25073877'], ['donald']))
    # c.start()
    # sleep(2)
    # v = multiprocessing.Process(target = startStream, args=(['scotus', 'supreme court', 'barrett'], [], ['supreme', 'court', 'amy', 'coney', 'trump']))
    # v.start()
    # sleep(2)
    startStream(['lakers', 'lebron', 'anthony davis'], ['20346956'], ['james'])
    # startStream(['trump', 'pence', 'realdonaldtrump'], ['25073877'], ['donald'])#, '759251', '1367531', '2836421', '2899773086'])
    p.join()
    # c.join()
    # v.join()
#    b.join()
    
#    TODO: Add a listener that will spawn a new stream on a new process.
        
        
        
        
        
        
