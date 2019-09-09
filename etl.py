from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType
import pyspark.sql.functions as F
import pandas as pd
from textblob.sentiments import NaiveBayesAnalyzer
import time
from textblob import Blobber
import traceback


def create_spark_session():
    """
    Return spark session. Initiate a spark session on AWS hadoop.

    :return: spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


def calc_sentiment(text_blobber, some_text):
    """
    Given an instantiate TextBlob and a tweet, calculate the sentiment

    :param text_blobber: instantiated TextBlob
    :param some_text: a twitter tweet
    :return: calculated TextBlob sentiment object
    """
    return text_blobber(some_text).sentiment


def process_tweets():
    """
    Given a folder of csv files by date of tweets, process each one and calculate the sentiment using TextBlob.
    Aggregate the data and return the final dataframe.

    :return: dataframe of daily calculate sentiment
    """
    FOLDER_PATH = r'years-of-crypto-data-master/bitcoin/New York'

    text_blobber = Blobber(analyzer=NaiveBayesAnalyzer())

    final_df = pd.DataFrame()

    for filename in os.listdir(FOLDER_PATH):
        print(filename)

        # Get tweets for current date
        try:
            tweets_df = pd.read_csv(os.path.join(FOLDER_PATH, filename), names=['timestamp', 'tweet', 'retweet'])

            # Filter bad data
            tweets_df = tweets_df.dropna(subset=['retweet'])

            # Format timestamp
            try:
                tweets_df.loc[:, 'tweet_timestamp'] = tweets_df['timestamp'].apply(
                    lambda x: datetime.strptime(x, '%a %b %d %H:%M:%S +0000 %Y'))
            except:
                import traceback
                print(traceback.format_exc())

            # Format date
            tweets_df.loc[:, 'tweet_date'] = tweets_df['tweet_timestamp'].apply(
                lambda x: datetime(x.year, x.month, x.day))

            # Filter by retweets
            filtered_tweets_df = tweets_df[tweets_df.retweet >= 50]

            # Compute sentiment
            filtered_tweets_df.loc[:, 'sentiment'] = filtered_tweets_df['tweet'].apply(
                lambda x: calc_sentiment(text_blobber, x))
            filtered_tweets_df.loc[:, 'classification'] = filtered_tweets_df['sentiment'].apply(
                lambda x: x.classification)
            filtered_tweets_df.loc[:, 'p_pos'] = filtered_tweets_df['sentiment'].apply(lambda x: x.p_pos)
            filtered_tweets_df.loc[:, 'p_neg'] = filtered_tweets_df['sentiment'].apply(lambda x: x.p_neg)

            # Weight and classification
            filtered_tweets_df.loc[:, 'weighted_sentiment'] = filtered_tweets_df.apply(lambda x: -1 * x['retweet']
                                                                    if x['classification'] == 'neg'
                                                                    else x['retweet'], axis=1)

            # Combine sentiment weights by date
            weighted_sentiment_df = filtered_tweets_df.groupby(['tweet_date']).sum()
        except:
            # print(traceback.format_exc())
            print('skipping {}'.format(filename))
            continue

        final_df = pd.concat([final_df, weighted_sentiment_df])

    return final_df


def process_historical_prices(spark_session):
    """
    Take four years of bitcoin prices from coinbase and calculate the daily performance returns

    :param spark_session:
    :return: dataframe of bitcoin daily performance returns
    """
    COINBASE_HIST_PRICES = r'bitcoin-historical-data/coinbaseUSD_1-min_data_2014-12-01_to_2019-01-09.csv'

    # Read in prices
    coinbase_hist_prices_df = spark_session.read.csv(COINBASE_HIST_PRICES, header=True)

    # Format timestamp
    coinbase_hist_prices_df = coinbase_hist_prices_df.withColumn('timestamp_final',
                                                                 F.col("Timestamp").cast("int").cast('timestamp'))
    # Format date
    coinbase_hist_prices_df = coinbase_hist_prices_df.withColumn('date', F.to_date('timestamp_final'))

    # Filter out where there is no Close price
    coinbase_hist_prices_df_filtered = coinbase_hist_prices_df.where(~F.isnan(F.col("Close")))

    # Create temp SQL table
    coinbase_hist_prices_df_filtered.createOrReplaceTempView("table1")

    # Get closing price per day
    coinbase_hist_prices_df_cob = spark_session.sql("select Close as close, table1.date \
        from table1 \
        join ( select table1.date, max(timestamp_final) as max_timestamp \
                from table1 \
                group by date ) temp ON table1.timestamp_final = temp.max_timestamp")

    # Order by date
    coinbase_hist_prices_df_cob = coinbase_hist_prices_df_cob.orderBy(coinbase_hist_prices_df_cob.date)

    # Convert to Pandas dataframe
    hist_prices_df = coinbase_hist_prices_df_cob.toPandas()
    # Convert price to float
    hist_prices_df['close'] = hist_prices_df['close'].astype(float)
    # Calc returns
    hist_prices_df['returns'] = (hist_prices_df['close'] - hist_prices_df.shift(1)['close']) / hist_prices_df['close']

    return hist_prices_df


def main():
    """
    The main function that drives all necessary processing.
    """
    # Process tweets
    tweets_df = process_tweets()

    # Process historical prices
    spark = create_spark_session()
    start_time = time.time()
    hist_prices_df = process_historical_prices(spark)
    print('process_historical_prices: {}'.format(time.time() - start_time))

    # Combine historical prices and tweets
    # We shift the tweets because we want to compare T-1 tweet sentiment with T price. The idea is that you will buy
    # or sell using T-1 sentiment on T so we need to compare T price/performance.
    merged_df = hist_prices_df.set_index('date').join(tweets_df.reset_index().set_index('tweet_date').shift(1),
                                                      how='inner')

    # Save output
    merged_df.to_csv('final.csv')


if __name__ == "__main__":
    main()
