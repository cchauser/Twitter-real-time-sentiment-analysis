# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 10:35:47 2020

@author: Cullen
"""

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer
from copy import deepcopy
import numpy as np
import itertools
import pandas as pd
import shelve
import re
import string

ttknzr = TweetTokenizer()
unknown_token = "UNKNOWN"
httpRegex = "@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+"

def clean_Markup(comment):
    comment = comment.lower()
    comment = re.sub(httpRegex, ' ', str(comment).strip()).strip()
    comment = comment.replace('*', '')
    comment = comment.replace('\n', ' ')
    comment = comment.replace('  ', ' ')
    return comment

def combine_Data(file1, file2):
    f1 = pd.read_csv(file1)
    f2 = pd.read_csv(file2)
    flag = True
    newData = []
    for i in range(80000):
        container = []
        try:
            if flag:
                container.append(clean_Markup(f1['Comment'][i//2]))
                container.append(int(f1['Label'][i//2]))
            elif not flag:
                container.append(clean_Markup(f2['Comment'][i//2]))
                container.append(int(f2['Label'][i//2]))
        except:
            continue
        newData.append(container)
        flag = not flag
    df = pd.DataFrame(newData)
    df.to_csv('fullData.csv', index = False, header = ['Comment', 'Label'])
    
def remove_stopWords(x_da, y_da):
    stop_words = set(stopwords.words('english'))

    i = 0
    while i < len(x_da):
        x_da[i] = clean_Markup(x_da[i])
        temp = ttknzr.tokenize(x_da[i])
        newX = ''
        for w in temp:
            if w not in stop_words:
                if w in string.punctuation:
                    newX = newX.strip() + w + ' '
                else:
                    newX += w + ' '
        x_da[i] = newX

        if len(x_da[i].split()) == 0:
            x_da.pop(i)
            y_da.pop(i)
            continue
        else:
            i += 1

    return x_da, y_da

def build_Vocab(fileName):
        fullData = pd.read_csv(fileName, names = ['score', 'id', 'date', 'flag', 'user', 'text'], encoding = 'ISO-8859-1')
        y_da = list(fullData['score'])
        x_da = list(fullData['text'])
        print(len(y_da))
        
        #Shuffle the data
        x_ = []
        y_ = []
        classCount = [0] * num_classes
        index = 0
        while classCount.count(800000) != num_classes:
            try:
                if classCount[y_da[index]//4] < 800000:
                    x_.append(x_da[index])
                    y_.append(y_da[index]//4)
                    classCount[y_da[index]//4] += 1
            except IndexError:
                if index > len(y_da):
                    break
                index += 1
                continue
            index += 1
        print(classCount)

        x_da, y_da = remove_stopWords(x_, y_)
        print('Stopwords removed. Remaining: ', len(x_da))
        x_stopw = deepcopy(x_da)
        y_stopw = deepcopy(y_da)
          
        allwords = [nltk.word_tokenize(comment) for comment in x_da]
             
        # Count the word frequencies
        word_freq = nltk.FreqDist(itertools.chain(*allwords))
        print("Found {} unique words tokens.".format(len(word_freq.items())))

        vocab_limit = min(int((len(word_freq.items()) * .2)), 40000)
         
        # Get the most common words and build index_to_word and word_to_index vectors
        vocab = word_freq.most_common(vocab_limit-1)
        i2w = [x[0] for x in vocab]
        i2w.append(unknown_token)
        i2w.append('PADDING')
        w2i = dict([(w,i) for i,w in enumerate(i2w)])

        print("Using a vocabulary size of {}.".format(len(w2i)))
        print("The least frequent word in the vocabulary is '{}', appearing {} times.".format(vocab[-1][0], vocab[-1][1]))

        print('Encoding words to indices')
        for i in range(len(x_da)):
            x_da[i] = nltk.word_tokenize(x_da[i])
            for w in range(len(x_da[i])):
                try:
                    x_da[i][w] = w2i[x_da[i][w]]
                except:
                    x_da[i][w] = w2i[unknown_token]

        y_d = []
        x_d = []
        S = [0] * num_classes
        print('Removing data points with too many unknown words')
        #This filters out all posts with more than 50% of their words unknown
        for i in range(len(y_da)):
            try:
                if((x_da[i].count(w2i[unknown_token])/len(x_da[i])) <= .5):
                    y_d.append(y_da[i])
                    S[y_da[i]] += 1
                    x_d.append(x_da[i])
            except:
                continue

        print(S)
        x, y = organizeData(x_d, y_d)
        x_data = np.asarray(x)
        y_data = np.asarray(y)

        print("\nSample size: {}".format(len(x_data)))
        return i2w, w2i, x_data, y_data, x_stopw, y_stopw
    
#Puts data in an order so that classes aren't all grouped together.
#Classes are put in order and then start over ie [0 1 2 0 1 2 0 1 2...]
def organizeData(x,y):
    for i in range(1, len(y)//2,2):
        temp = [deepcopy(x[i]), deepcopy(y[i])]
        x[i] = x[-i]
        y[i] = y[-i]
        x[-i] = temp[0]
        y[-i] = temp[1]
    return x, y

num_classes = 2

#f = input("FILENAME: ")
i2w, w2i, x, y, x_stopw, y_stopw = build_Vocab('sentiment/training.csv')
x_stopw, y_stopw = organizeData(x_stopw, y_stopw)

db = shelve.open('sentiment/data')
db['i2w'] = i2w
db['w2i'] = w2i
db['x'] = x
db['y'] = y
db['xstop'] = x_stopw
db['ystop'] = y_stopw
db.close()
print("DONE")


