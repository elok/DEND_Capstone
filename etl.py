import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType
import pyspark.sql.functions as F
import pandas as pd
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer

# Read credentials from config file
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Return spark session
    Initiate a spark session on AWS hadoop
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


# def process_song_data(spark, input_data, output_data):
#     """
#     Process song_input data using spark and output as parquet to S3
#     Input: spark session, song_input_data json, and output_data S3 path
#     """
#     # read song data file
#     song_df = spark.read.json(input_data).dropDuplicates()
#     print(song_df.count())
#     song_df.show(5)
#
#     # extract columns to create songs table
#     songs_filtered_df = song_df.select('song_id', 'title', 'artist_id', 'year', 'duration')
#     songs_filtered_df.show(5)
#
#     # write songs table to parquet files partitioned by year and artist
#     print('Writing to parquet')
#     songs_filtered_df.write.partitionBy('year', 'artist_id').parquet("{}/songs".format(output_data), 'overwrite')
#
#     # songs_filtered_df.write.partitionBy('year', 'artist_id').parquet('/Users/ericlok/Downloads/song_data', 'overwrite')
#
#     # extract columns to create artists table
#     artists_table = song_df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
#     print(artists_table.show(5))
#
#     # write artists table to parquet files
#     print('Writing to parquet')
#     artists_table.write.parquet("{}/artist_table/artist_table.parquet".format(output_data), 'overwrite')


# def process_log_data(spark, input_data, output_data, song_input_data):
#     """
#     Process log_input_data data using spark and output as parquet to S3
#     Input: spark session, log_input_data json, song_input_data json, and output_data S3 path
#     """
#     # read log data file
#     log_data_df = spark.read.json(input_data).dropDuplicates()
#     log_data_df.show(5)
#
#     # filter by actions for song plays
#     log_data_df = log_data_df.filter(log_data_df['page'] == 'NextSong')
#     log_data_df.show(5)
#
#     # extract columns for users table
#     users_table = log_data_df.select('userId', 'firstName', 'lastName', 'gender', 'level')
#     print(users_table.show(5))
#
#     # write users table to parquet files
#     users_table.write.parquet("{}/users_table/users_table.parquet".format(output_data), 'overwrite')
#
#     # create timestamp column from original timestamp column
#     get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000), TimestampType())
#     log_data_df = log_data_df.withColumn("timestamp", get_timestamp(log_data_df.ts))
#     log_data_df.printSchema()
#     log_data_df.show(5)
#
#     # create datetime column from original timestamp column
#     get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000), DateType())
#     log_data_df.show(5)
#     log_data_df = log_data_df.withColumn("start_time", get_datetime(log_data_df.ts))
#     log_data_df.printSchema()
#     log_data_df.show(5)
#
#     # extract columns to create time table
#     log_data_df = log_data_df.withColumn("hour", F.hour("timestamp"))
#     log_data_df = log_data_df.withColumn("day", F.dayofweek("timestamp"))
#     log_data_df = log_data_df.withColumn("week", F.weekofyear("timestamp"))
#     log_data_df = log_data_df.withColumn("month", F.month("timestamp"))
#     log_data_df = log_data_df.withColumn("year", F.year("timestamp"))
#     log_data_df = log_data_df.withColumn("weekday", F.dayofweek("timestamp"))
#
#     time_table = log_data_df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
#     time_table.show(5)
#     time_table.createOrReplaceTempView("time_table")
#
#     # write time table to parquet files partitioned by year and month
#     time_table.write.partitionBy('year', 'month').parquet("{}/time_table/time_table.parquet".format(output_data), 'overwrite')
#
#     # read in song data to use for songplays table
#     song_df = spark.read.json(song_input_data).dropDuplicates()
#
#     # create temp table
#     song_df.createOrReplaceTempView('song_df_table')
#     log_data_df.createOrReplaceTempView('log_data_df_table')
#
#     # extract columns from joined song and log datasets to create songplays table
#     songplays_table = spark.sql("""
#         SELECT time_table.year, time_table.month, log_data_df_table.start_time, log_data_df_table.userId, log_data_df_table.level,
#             log_data_df_table.sessionId, log_data_df_table.location, log_data_df_table.userAgent,
#             song_df_table.song_id, song_df_table.artist_id
#         FROM log_data_df_table
#         INNER JOIN song_df_table
#         ON song_df_table.artist_name = log_data_df_table.artist
#         INNER JOIN time_table
#         ON time_table.start_time = log_data_df_table.start_time
#     """)
#
#     # write songplays table to parquet files partitioned by year and month
#     songplays_table.write.partitionBy('year', 'month').parquet('/Users/ericlok/Downloads/song_play', 'overwrite')


def main():
    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    # log_input_data = "s3a://udacity-dend/log_data/*/*/*.json"
    # output_data = "s3a://eric-data-lake-west/"

    # input_data = "data/song_data/A/A/A/*.json"
    # log_input_data_test = "data/log-data/*.json"

    # process_song_data(spark, input_data, output_data)
    # process_log_data(spark, log_input_data, output_data, input_data)

    spark.textFile(r'C:\workspace\dend_capstone\years-of-crypto-data-master\bitcoin\New York')

def calc_sentiment(some_text):
    blob = TextBlob(some_text, analyzer=NaiveBayesAnalyzer())
    return blob.sentiment

def process_tweets(spark_session):
    FOLDER_PATH = r'C:\workspace\dend_capstone\years-of-crypto-data-master\bitcoin\New York'

    for filename in os.listdir(FOLDER_PATH):
        # Get tweets for current date
        tweets_df = pd.read_csv(os.path.join(FOLDER_PATH, filename), names=['timestamp', 'tweet', 'retweet'])

        # Save the dates
        tweets_df['tweet_timestamp'] = tweets_df['timestamp'].apply(
            lambda x: datetime.strptime(x, '%a %b %d %H:%M:%S +0000 %Y'))
        tweets_df['tweet_date'] = tweets_df['tweet_timestamp'].apply(lambda x: datetime(x.year, x.month, x.day))

        # Filter by retweets
        filtered_tweets_df = tweets_df[tweets_df.retweet >= 50]

        # Compute sentiment
        filtered_tweets_df['sentiment'] = filtered_tweets_df['tweet'].apply(lambda x: calc_sentiment(x))
        filtered_tweets_df['classification'] = filtered_tweets_df['sentiment'].apply(lambda x: x.classification)
        filtered_tweets_df['p_pos'] = filtered_tweets_df['sentiment'].apply(lambda x: x.p_pos)
        filtered_tweets_df['p_neg'] = filtered_tweets_df['sentiment'].apply(lambda x: x.p_neg)

def process_historical_prices(spark_session):
    COINBASE_HIST_PRICES = r'C:\workspace\dend_capstone\bitcoin-historical-data\coinbaseUSD_1-min_data_2014-12-01_to_2019-01-09.csv'

    # Read in prices
    coinbase_hist_prices_df = spark_session.read.csv(COINBASE_HIST_PRICES, header=True)
    # coinbase_hist_prices_df = pd.read_csv(COINBASE_HIST_PRICES, engine='python')

    # Format dates
    coinbase_hist_prices_df = coinbase_hist_prices_df.withColumn('timestamp_final', F.col("Timestamp").cast("int").cast('timestamp'))

    #

def main2():
    spark = create_spark_session()

    # process_tweets()
    process_historical_prices(spark)




if __name__ == "__main__":
    # main()
    main2()
