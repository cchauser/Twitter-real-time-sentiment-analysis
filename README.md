# Twitter real-time sentiment analysis

This project uses:

* Tweepy
* Kafka (on your localmachine)
* Keras
* Tensorflow
* Dash
* Plotly
* Pandas

You can find a quickstart guide to setting up Kafka [here](https://kafka.apache.org/quickstart).

The neural network was trained using the [sentiment140](https://www.kaggle.com/kazanova/sentiment140) dataset.

### About this project

The program reads a stream of tweets, replies, and mentions pertaining to a set of keywords or in response to tweets by specified Twitter accounts. TwitterStream.py then partially cleans the text before sending it to the master consumer using a Kafka stream. MasterConsumer.py then cleans the text further and preps it for analysis by a neural network and keyword extraction. Users that are followed have their tweets send to the frontend by the master consumer at this time as well.

When the data reaches the front end (consisting of Dash and plotly) it is organized into pandas dataframes and graphed on a webpage.

Below you can see an example of the end result when the program was used to analyze twitter for SpaceX's attempted launch on May 27th, 2020.

![Graphing dashboard for SpaceX launch](https://github.com/cchauser/Twitter-real-time-sentiment-analysis/blob/master/spacexlaunchFull.png)

You can also see where certain events occured during the course of the attempted launch:

![Timeline of events](https://github.com/cchauser/Twitter-real-time-sentiment-analysis/blob/master/spacexlaunch.png)
