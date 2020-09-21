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
import multiprocessing
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



# we create this class that inherits from the StreamListener in tweepy StreamListener
class TweetsListener(StreamListener):
    
    def __init__(self, keywordArg, userArg):
        self.userArg = userArg
        self.keywordArg = keywordArg
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
                        
                    if len(tweet) < 3:
                        return True
                    
                    
#                    print(self.header)
                    packet = {'topic': [self.topic], 'tweet': tweet, 'terms': self.keywordArg}
                    self.producer.send('TwitterStream', value=packet)
            return True
        except Exception as e:
            # pass
            print("Error on_data: %s" % str(e))
        return True

    def if_error(self, status):
        print(status)
        return True
    
    def on_exception(self, exception):
        print(exception)
        return True


def startStream(keywords, users):
    print('Starting stream using', keywords)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    twitter_stream = Stream(auth, TweetsListener(keywords, users))
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
    p = multiprocessing.Process(target = startStream, args=(['biden', 'kamala'], ['939091']))
    p.start()
    sleep(1)
    c = multiprocessing.Process(target = startStream, args=(['trump', 'pence'], ['25073877']))
    c.start()
    sleep(1)
    startStream(['nba', 'playoffs', 'basketball', 'basket ball'], ['19923144'])#, '759251', '1367531', '2836421', '2899773086'])
    p.join()
    c.join()
#    v.join()
#    b.join()
    
#    TODO: Add a listener that will spawn a new stream on a new process.
        
        
        
        
        
        
