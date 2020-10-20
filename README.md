# Twitter real-time sentiment analysis

This project uses:

* Tweepy
* Kafka
* Keras
* Tensorflow
* Dash
* Plotly
* NLTK
* mySQL


This project has been launched! You can see it run at [chauser.me](https://chauser.me).

You can find a quickstart guide to setting up Kafka [here](https://kafka.apache.org/quickstart). You'll need two topics. One called "TwitterStream" and one called "FrontEnd". You will also need a set of Twitter API keys that can be obtained [here](https://developer.twitter.com/en/docs/basics/getting-started). Save your keys in a txt file named 'twitterkeys.txt' with each key/token on a seperate line.

The neural network was trained using the [sentiment140](https://www.kaggle.com/kazanova/sentiment140) dataset. The final trained neural network is too large to upload to GitHub so I've provided the program used to train it in this repo. You can either provide your own model or train one using my code. To train one with my code you'll need to first run datacleaner.py, then run TwittSentTrainer.py. You may need to alter datacleaner.py to point to the sentiment140 dataset on your machine.

### About this project

The program reads a stream of tweets, replies, and mentions pertaining to a set of keywords or in response to tweets by specified Twitter accounts. TwitterStream.py then partially cleans the text before sending it to the master consumer using a Kafka stream. MasterConsumer.py then cleans the text further and preps it for analysis by a neural network, keyword extraction, and analysis of users being followed. Once the consumer is done it uploads the data to a mySQL server. The front end then selects the data it wants from the SQL server and displays it on the dashboard.

This is what the website looks like if you're unable to visit it for some reason:

![Website dashboard](https://github.com/cchauser/Twitter-real-time-sentiment-analysis/blob/master/chauser_2020-10-06_18-40-22.png)

It's important to keep in mind that just because some tweets are labelled as negative does not mean they are hateful of the given topic. They may be expressing disappointment or sadness which the neural network considers to be a negative sentiment.

### Reading the graphs

All of the graphs on the dashboard update every minute but the consumer only processes and pushes data every two minutes. It's highly recommended to leave the consumer processing time at two minutes to reduce dips and spikes in the stream.

#### Line graph

The line graph graphs the sentiment output from the neural network. The y-axis is the number of tweets/replies, the x-axis is a time series.

If you notice a large spike in any of the lines outside of normal trends it's probably because someone sent out a spicy tweet! Check the table!

#### Bar graph

The bar graph shows frequency of keywords that people are using in association with the keywords you're looking for.

It also shows the amount of tweets with each sentiment that are using those keywords. So not only are you able to see what people are talking about you're able to see how they feel about each of those words!

#### Table

The only tweets that show up here are the tweets sent by the users being followed. You can change who is followed by editing the target_users variable in TwitterStream.py. *I'll add a better way to change it in the future*

Date: yyyy-mm-dd HH:MM:SS

User: This is the twitter user who sent the tweet.

Text: What the user tweeted.

Sentiment Change: For each tweet the sentiment scores for two minute window prior to the tweet are saved. Every two minutes those sentiment scores are compared to the new sentiment scores and the percentage of change is reflected here. These numbers are updated for the next five minutes. After five minutes the percentage of change is no longer updated.

Activity Change: Same as sentiment change but this measures change in total number of keyword mentions, tweets, and replies.

### How to run it

At the moment there is no main program that can be run to launch the entire program. In the future I hope to have a main file that launches each program in its own process, but for now you will need to run TwitterStream.py, MasterConsumer.py, and frontendDash.py in seperate terminals/interpreters.
