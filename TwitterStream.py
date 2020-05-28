# -*- coding: utf-8 -*-
"""
Created on Thu May  7 09:57:30 2020

@author: Cullen
"""

from tweepy.auth import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import KafkaProducer
from time import time
import json
import os
import re

with open('twitterkeys.txt') as f:
    keys = f.readlines()

consumer_key = keys[0].replace('\n', '')
consumer_secret= keys[1].replace('\n', '')
access_token= keys[2].replace('\n', '')
access_secret= keys[3].replace('\n', '')

httpRegex = "@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+"

searchTerms = ['spacex', 'space x', 'nasa', 'crew dragon', 'falcon 9', 'bob behnken', 'behnken', 'doug hurley', 'hurley',
               'astronaut', 'international space station', 'space station', 'cape canaveral', 'launchamerica']
targetUsers = ['11348282', '34743251', '16580226', '1451773004', '44196397']


# we create this class that inherits from the StreamListener in tweepy StreamListener
class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket
        
        
    # we override the on_data() function in StreamListener
    def on_data(self, data):
        try:
            message = json.loads(data)
            try:
                #This filters out retweets. I only want OC
                message['retweeted_status'] #retweeted_status is only present in the JSON if the tweet is not a retweet it will raise a keyerror
                return True
            except:
                #Twitter clips off long tweets and flags as truncated then moves them to an extended tweet section in the json
                if message['truncated']:
                    tweet = message['extended_tweet']['full_text']
                else:
                    tweet = message['text']
                    
                    
                if message['user']['id_str'] in targetUsers:
                    print(tweet, message['user']['screen_name'])
                    packet = {'time': time(), 'image_url': message['user']['profile_image_url'], 'user': message['user']['screen_name'], 'tweet': tweet}
                    self.client_socket.send('TwitterStream', value=packet, headers = [('twitterFollow', b'1')])
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
                    
                    
                    print(tweet)
                    packet = {'tweet': tweet, 'terms': searchTerms}
                    self.client_socket.send('TwitterStream', value=packet, headers = [('twitterTrack', b'1')])
            return True
        except Exception as e:
            pass
#            print("Error on_data: %s" % str(e))
        return True

    def if_error(self, status):
        print(status)
        return True
    
    def on_exception(self, exception):
        print(exception)
        return True


def send_tweets(producer):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    twitter_stream = Stream(auth, TweetsListener(producer))
    while True:
        try:
            twitter_stream.filter(languages = ['en'], track=searchTerms, follow = targetUsers) # start the stream
        except KeyboardInterrupt:
            print('Keyboard Interrupt')
            return True
        except Exception as e:
            print(e)
            print("IM HERE")
            continue



if __name__ == "__main__":
    
    print("Initializing Kafka Producer")
    producer = KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_HOST', 'localhost:9092'),
                             value_serializer = lambda x: json.dumps(x).encode('utf-8'),
                             batch_size = 0)
    if producer.bootstrap_connected():
        print('Connected')
        send_tweets(producer)
    else:
        print('Failed to connect to service')