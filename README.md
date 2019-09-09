# Project: Data Engineering Capstone Project

## Project Description
This is the capstone project for the Udacity Data Engineering Nanodegree.

I have chosen the open ended project option and in this project, I will apply what I've learned on Spark and data lakes to build an ETL pipeline. To complete the project, I retrieved two datasets from Kaggle on Bitcoin, process the data into analytics tables using Spark, and output them to csv files.

The main goal is to determine if Twitter tweets has any impact on the price movements of Bitcoin. The idea is that many users subscribe to bitcoin related tweets and react emotionally.
The analysis of the data can be done in many ways:
* Use positive sentiment only
* Use negative sentiment only
* Use a combination of both positive and negative sentiments
* Use number of retweets as some sort of weight indicator

--------------------------------------------

## Data Sets
There are two datasets that I used from Kaggle:

Bitcoin Historical Data<br>
Bitcoin data at 1-min intervals from select exchanges, Jan 2012 to August 2019<br>
https://www.kaggle.com/mczielinski/bitcoin-historical-data

Twitter Parsed Cryptocurrencies Data<br>
Parsed crypto tags from twitter<br>
https://www.kaggle.com/johnyleebrown/twitter-parsed-cryptocurrencies-data

--------------------------------------------

## Libraries
In order to calculate a sentiment based on a tweet, I have used the library called TextBlob. TextBlob is a Python (2 and 3) library for processing textual data. It provides a simple API for diving into common natural language processing (NLP) tasks such as part-of-speech tagging, noun phrase extraction, sentiment analysis, classification, translation, and more.

One of the functions that TextBlob provides is sentiment analysis. The textblob.sentiments module contains two sentiment analysis implementations, PatternAnalyzer (based on the pattern library) and NaiveBayesAnalyzer (an NLTK classifier trained on a movie reviews corpus).

NaiveBayesAnalyzer returns its result as a namedtuple of the form: Sentiment(classification, p_pos, p_neg). I will be using this.

https://textblob.readthedocs.io/en/dev/

--------------------------------------------

## Analytics tables
Using the bitcoin twitter and price datasets, the idea is to create a star schema optimized for queries on sentiment analysis. There are two dimension tables, one for price history and performance and the other for twitter sentiments. Due to time constraint, I could not actually create the tables and load them but this is how I would do. The data sits in pandas dataframes.  

#### Dimension Tables

* price_returns - table daily price performance
  * close - the closing end of day price
  * date - the date of the close price
  * returns - the percent difference between today's price and yesterday's price (T - T-1) / T

* twitter_sentiment - table of summarized daily twitter sentiments
  * date - the date of the summarized twitter sentiments
  * retweet - the number of retweets a current tweet has
  * p_pos - the sum of all positive sentiment calculated by TextBlob across all tweets for that date
  * p_neg - the sum of all negative sentiment calculated by TextBlob across all tweets for that date
  * weighted_sentiment - the product of retweets and sentiment (retweets * sentiment). if there are 120 retweets and the sentiment is negative then the weight sentiment is -120.

#### Fact Table

* returns_sentiment - daily price performance with the twitter sentiment from the previous day
  * date - the date of the price performance
  * close - the closing end of day price
  * returns - the percent difference between today's price and yesterday's price (T - T-1) / T
  * retweet - the total number of retweets for that day
  * p_pos - the sum of all positive sentiment calculated by TextBlob across all tweets for that date
  * p_neg - the sum of all negative sentiment calculated by TextBlob across all tweets for that date
  * weighted_sentiment - the product of retweets and sentiment (retweets * sentiment). if there are 120 retweets and the sentiment is negative then the weight sentiment is -120.

--------------------------------------------

## How to run
After opening terminal session, set your filesystem on project root folder and insert these commands in order to run the demo: <br>

<I> And this will execute our ETL process </I>
`` python etl.py``

--------------------------------------------

## Project structure
This is the project structure, if the bullet contains ``/`` means that the resource is a folder:

* <b> etl.py </b> - This script executes the code reads the CSV data files from the Kaggle datasets, process the data into their respective fact and dimension dataframes using Spark.
* <b> conda_dend_env.yml </b> - The anaconda environment file
* <b> final.csv </b> - The final result output

## Improvements / Next Steps
* Due to time constraint, this project was really a proof of concept. If I had to productionize this, I would do the following: 
  * I have only loaded the tweets from New York, I would parallelize the process by using spark to load all the cities.
  * Another option is I could upload all the data into AWS S3 and then process them using an AWS EMR cluster.
* Airflow / Daily Runs
  * I don't have a way to retrieve daily data so it's difficult to talk about how to incorporate Airflow. If the data was available, AirFlow would be a perfect tool to streamline the process. It would work something like this.
    1a. Retrieve closing price
    1b. Retrieve tweets for same day and compute sentiment
    2.  Combine both data set and insert new date values into existing database
* How often the data should be updated?
  * Once a day. I have chosen a low frequency model. The tweets are actually intra-day, however, the pricing data is daily so we can only analyze daily. 
* If the data was increased by 100x:
  * My code would be extremely slow. The data would need to be split and run in a cluster. What's great is that the data is by data and could be split up easily. 
* If the database needed to be accessed by 100+ people:
  * My two options for cloud database storage is AWS Redshift and AWS S3. I honestly think either would be fine for 100x people.