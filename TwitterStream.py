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
from configparser import ConfigParser
from multiprocessing import Process, Pipe
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


def determineTopic(topicDict, tweet, childPipe):
    tweetTopics = []
    for topic, data in topicDict.items():
        # Data[0] is the keywords
        # Data[1] is user strings
        # Data[2] is filter words
        if tweet['user']['id_str'] in data[1]:
            childPipe.send(topic)
        try:
            if tweet['in_reply_to_user_id_str'] in data[1]:
                tweetTopics.append(topic)
                continue
        except KeyError:
            pass
        
        if bool(tweet['truncated']):
            tweetText = tweet['extended_tweet']['full_text']
        else:
            tweetText = tweet['text']
        tweetText = word_tokenize(tweetText.lower())
        if len(data[0].intersection(set(tweetText))) > 0:
            tweetTopics.append(topic)
    childPipe.send(tweetTopics)
    return True
                

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
    
    def __init__(self, topicDict, userArg):
        self.topicDict = topicDict
        self.userArg = userArg
        self.producer = KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_HOST', 'localhost:9092'),
                             value_serializer = lambda x: json.dumps(x).encode('utf-8'),
                             batch_size = 0)
        self.parentPipe, self.childPipe = Pipe()
        
        
    # we override the on_data() function in StreamListener
    def on_data(self, data):
        message = json.loads(data)
        if 'limit' in message:
            return True
        try:
            if 'retweeted_status' in message:
                return True
            else:
                topicProcess = Process(target = determineTopic, args=[self.topicDict, message, self.childPipe])
                topicProcess.start()
                
                #Twitter clips off long tweets and flags as truncated then moves them to an extended tweet section in the json
                if bool(message['truncated']):
                    tweet = message['extended_tweet']['full_text']
                else:
                    tweet = message['text']

                if message['user']['id_str'] in self.userArg:
                    tweet = tweet.encode('ASCII', 'ignore').decode() #Removes emojis
                    while '  ' in tweet:
                        tweet = tweet.replace('  ', ' ')
                        
                    topics = self.parentPipe.recv()
                    topicProcess.join()
                    
                    print(tweet, message['user']['screen_name'])
                    packet = {'topic': ['userTrack', topics], 'time': int(time()), 'user': message['user']['screen_name'], 'tweet': tweet}
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
                    
                    topicProcess.join()
                    topics = self.parentPipe.recv()
                    
                    if len(tweet) < 3:
                        return True
                    elif len(topics) == 0:
                        return True

                    for topic in topics:
                        packet = {'topic': [topic], 'tweet': tweet, 'terms': list(self.topicDict[topic][0])}
                        self.producer.send('TwitterStream', value=packet)
            return True
        except Exception as e:
#             pass
            print("Error on_data: %s" % str(e))
            print(message)
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
def startStream(topicDict, keywords, users):
    print('Starting stream using', keywords)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    print('Authorization successful: ', auth.oauth.verify)
    
    
    while True:
        twitter_stream = Stream(auth, TweetsListener(topicDict, users))
        try:
            twitter_stream.filter(languages = ['en'], track=keywords, follow = users) # start the stream
        except KeyboardInterrupt:
            print('Keyboard Interrupt')
            return True
        except Exception as e:
            print(e)
            print("IM HERE")
            continue
        finally:
            # we do this because tweepy streams never actually clear their queued tweets when they stop/crash
            # this way if something goes wrong we start fresh. We might lose some data but at least it'll keep running
            del twitter_stream

def loadStreamConfig(file):
    config = ConfigParser()
    config.read(file)
    topics = config.sections()
    keywords, users = [], []
    topicDict = {}
    for topic in topics:
        kw = config[topic]['keywords'].split(',')
        usr = config[topic]['users'].split(',')
        fwords = config[topic]['filter'].split(',')
        keywords += kw
        users += usr
        topicDict[topic] = [set(kw+fwords), usr]
    return topicDict, keywords, users




if __name__ == "__main__":
    d, kw, usr = loadStreamConfig('streamConfig.ini')
    
    startStream(d, kw, usr)
    
#    TODO: Add a listener that will spawn a new stream on a new process.
        
        
        
        
        
        
