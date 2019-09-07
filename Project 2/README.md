# Project 2: Data Warehousing in Redshift (IaC)

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Purpose
This project aims at building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

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

## Data sources and repo files
For the purpose of this exercise, the repo only contains information related to the ETL and Redshift processes. Information about the data sources can be found here [Song Dataset](http://millionsongdataset.com/) and here [Log Dataset](https://github.com/Interana/eventsim):

- **sql_queries.py:** defines the schema of the five tables and the corresponding SQL queries, which will be imported into the two other files above.
- **create_tables.py:** triggers the creation of the Database and the five afore mentioned tables for the star schema in Redshift.
- **elt.py:** load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
- **Project IaC script.ipynb:** Loads the DB Params from the config file; creates clients for EC2, S3, IAM and Redshift; checks status and cleans up all the resources
- **dwh.cfg:** Contains all config relevant information e.g. AWS keys, Cluster, IAM role and S3

## Project Datasets
We work with two datasets that reside in S3. Here are the S3 links for each:

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Log data json path: s3://udacity-dend/log_json_path.json

## Required Python libraries
- configparser
- psycopg2
 
## Running the jobs
```
$ python create_tables.py
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


