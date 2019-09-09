# Project: Data Engineering Capstone Project

## Project Description
This is the capstone project for the Udacity Data Engineering Nanodegree. 

I have chosen the open ended project option and in this project, I will apply what I've learned on Spark and data lakes to build an ETL pipeline. To complete the project, I retrieved two datasets from Kaggle on Bitcoin, process the data into analytics tables using Spark, and output them to csv files.

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

## Analytics tables
Using the bitcoin twitter and price datasets, I created a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table

songplays - records in log data associated with song plays i.e. records with page NextSong <br>
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent <br>

#### Dimension Tables

users - users in the app<br>
user_id, first_name, last_name, gender, level<br>
songs - songs in music database<br>
song_id, title, artist_id, year, duration<br>
artists - artists in music database<br>
artist_id, name, location, lattitude, longitude<br>
time - timestamps of records in songplays broken down into specific units<br>
start_time, hour, day, week, month, year, weekday<br>

--------------------------------------------

## How to run
After opening terminal session, set your filesystem on project root folder and  insert these commands in order to run the demo: <br>

<I> And this will execute our ETL process </I>
`` python etl.py``

--------------------------------------------

## Project structure
This is the project structure, if the bullet contains ``/`` means that the resource is a folder:

* <b> etl.py </b> - This script executes the queries that extract JSON data from the S3 bucket, process the data into their respective fact and dimension tables using Spark, and then load the parquet files back into S3.
* <b> dl.cfg </b> - Configuration file used that contains AWS IAM credentials
