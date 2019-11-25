# Project 3: Data Lake in AWS S3 / EMR

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Purpose
This project aims at building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

For this a predifined ETL process is executed and star schema is chosen as the more flexible and convenient approach to the analytical requirements e.g. (DWH Fundamentals: A comprehensive guide for IT professionals)

- Easy for users to understand
- Optimizes navigation
- Most suitable for query processing
- Enables specific performance schemes

The fact table is defined as the songplay list and four dimension tables are created with information about:

#### Fact Table
1. **songplays** - records in event data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
2. **users** - users in the app
- user_id, first_name, last_name, gender, level
3. **songs** - songs in music database
- song_id, title, artist_id, year, duration
4. **artists** - artists in music database
- artist_id, name, location, lattitude, longitude
5. **time** - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday

The chart below displays this structure:

#### Database Schema
![alt text](https://github.com/phidesigner/DE_nanodegree/blob/master/Project%201/Pics/ERD.png)

#### Serialization/ storage compression

Parquet is used as the compression file format for storage due to it's increase in performance over other formats (for this use case), namely:

- Organizing by column allows for better compression, as data is more homogeneous. The space savings are very noticeable at the scale of a Hadoop cluster.
- I/O will be reduced as we can efficiently scan only a subset of the columns while reading the data. Better compression also reduces the bandwidth required to read the input.
- As we store data of the same type in each column, we can use encoding better suited to the modern processors’ pipeline by making instruction branching more predictable.

Source: [Acadgild](https://acadgild.com/blog/parquet-file-format-hadoop)

## Data sources and repo files
For the purpose of this exercise, the repo only contains information related to the ETL to process data to and from the S3 bucket, via PySpark scripts. Information about the data sources can be found here [Song Dataset](http://millionsongdataset.com/) and here [Log Dataset](https://github.com/Interana/eventsim):

- **elt.py:** loads and saves the data from S3 and then processes that data into analytics tables saved as parquet format.
- **dl.cfg:** Contains all config relevant information e.g. AWS key and password (xxx)
- **output files**:
    - artists.parquet
    - songplays.parquet
    - songs.parquet
    - time.parquet
    - users.parquet

## Project Datasets
We work with two datasets that reside in S3. Here are the S3 links for each:

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data

## Required Python libraries
- configparser
- datetime
- os
- pyspark
 
## Running the jobs
```
$ python etl.py
```
## Test and validation of the output

SELECT COUNT(*) FROM staging_events
56392
SELECT COUNT(*) FROM staging_songs
104272
SELECT COUNT(*) FROM songplays
1630
SELECT COUNT(*) FROM users
520
SELECT COUNT(*) FROM songs
59584
SELECT COUNT(*) FROM artists
30075
SELECT COUNT(*) FROM time
2

## Tables and query examples
#### Songplays table
![alt text](https://github.com/phidesigner/DE_nanodegree/blob/master/Project%201/Pics/songplays.png)

#### Users table
![alt text](https://github.com/phidesigner/DE_nanodegree/blob/master/Project%201/Pics/users.png)

#### Songs table
![alt text](https://github.com/phidesigner/DE_nanodegree/blob/master/Project%201/Pics/songs.png)

#### Artists table
![alt text](https://github.com/phidesigner/DE_nanodegree/blob/master/Project%201/Pics/artists.png)

#### Time table
![alt text](https://github.com/phidesigner/DE_nanodegree/blob/master/Project%201/Pics/time.png)


