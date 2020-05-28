# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 12:33:45 2020

@author: Cullen
"""

import re
import nltk
import numpy as np
import shelve
import gensim
import pickle
from tensorflow.python.keras.preprocessing.sequence import pad_sequences
from nltk.tokenize import TweetTokenizer
from keras.preprocessing.text import Tokenizer
from keras.models import Sequential
from keras.layers import Dense, Embedding, LSTM, Dropout
from sklearn.preprocessing import LabelEncoder
from gensim.models import Word2Vec

httpRegex = "@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+"

np.random.seed(7)

ttknzr = TweetTokenizer()

db = shelve.open('sentiment/data', 'r')
#i2w = db['i2w']
#w2i = db['w2i']
x = db['xstop']
y = db['ystop']
db.close()

trainCutoff = int(((len(x) * .95)//128)*128)
x_train = x[:trainCutoff]
y_train = y[:trainCutoff]
x_test = x[trainCutoff:trainCutoff+30720]
y_test = y[trainCutoff:trainCutoff+30720]

W2V_SIZE = 256
W2V_WINDOW = 7
W2V_EPOCH = 32
W2V_MIN_COUNT = 10


try:
    w2v_model = Word2Vec.load('W2V_TwittSent.w2v')
    print('W2V Model Loaded Successfully')
except:
    print('Model not found. Training new W2V model')
    
    print("Regexing out tags and links")
    for i in range(len(x_train)):
        x_train[i] = re.sub(httpRegex, ' ', str(x_train[i]).strip()).strip()
    for i in range(len(x_test)):
        x_test[i] = re.sub(httpRegex, ' ', str(x_test[i]).strip()).strip()
    
    print("Creating w2v model")
    w2v_model = gensim.models.word2vec.Word2Vec(size=W2V_SIZE, 
                                                window=W2V_WINDOW, 
                                                min_count=W2V_MIN_COUNT, 
                                                workers=8)
    
    documents = [ttknzr.tokenize(_text) for _text in x_train]
    
    print("Building vocab")
    w2v_model.build_vocab(documents)
    words = w2v_model.wv.vocab.keys()
    vocab_size = len(words)
    print("Vocab size", vocab_size)
    
    print("Training w2v model")
    w2v_model.train(documents, total_examples=len(documents), epochs=W2V_EPOCH)
    
    w2v_model.save('W2V_TwittSent.w2v')
    print("W2V model generated and saved")



tokenizer = Tokenizer()
tokenizer.fit_on_texts(x_train)

vocab_size = len(tokenizer.word_index) + 1

encoder = LabelEncoder()
encoder.fit(y_train)

y_train = encoder.transform(y_train)
y_test = encoder.transform(y_test)

y_train = y_train.reshape(-1,1)
y_test = y_test.reshape(-1,1)

print("y_train",y_train.shape)
print("y_test",y_test.shape)

embedding_matrix = np.zeros((vocab_size, W2V_SIZE))
for word, i in tokenizer.word_index.items():
  if word in w2v_model.wv:
    embedding_matrix[i] = w2v_model.wv[word]
print(embedding_matrix.shape)


print("Creating embedding layer")
embedding_layer = Embedding(vocab_size, W2V_SIZE, weights=[embedding_matrix], input_length=70, trainable=False)



max_len = max([len(s) for s in x])

print("Padding sequences")
x_train = pad_sequences(tokenizer.texts_to_sequences(x_train),padding='pre', maxlen=70)
x_test = pad_sequences(tokenizer.texts_to_sequences(x_test),padding='pre', maxlen=70)

print("Creating sequential model")
model = Sequential()
print('embed')
model.add(embedding_layer)
print('drop')
model.add(Dropout(.5))
print('lstm1')
model.add(LSTM(units = 512, return_sequences = True, dropout = 0.5, recurrent_dropout = 0.5))
print('lstm2')
model.add(LSTM(units = 512, dropout = 0.25, recurrent_dropout = 0.25))
print('dense out')
model.add(Dense(1, activation = 'sigmoid'))

print("COMPILING MODEL")
model.compile(loss = 'binary_crossentropy', optimizer = 'adam', metrics=['accuracy'])

print("TRAINING MODEL")
model.fit(x_train, y_train, batch_size = 1024, epochs = 10, validation_split = 0.1, verbose = 2)


print('Saving models')
model.save('TwitSent.h5')
w2v_model.save('W2V_TwittSent.w2v')
pickle.dump(tokenizer, open('Toke_TwittSent.pkl', "wb"), protocol=0)
pickle.dump(encoder, open('Encode_TwittSent.pkl', "wb"), protocol=0)
print('Models saved')