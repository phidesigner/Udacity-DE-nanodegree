# Project 4: Data Pipelines with Airflow & Redshift

## Introduction
A music streaming startup, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow. Their data resides in S3, and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Purpose
This project aims at creating high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. As data quality plays a big part when analyses are executed on top the data warehouse, tests are ran against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

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
6. **Stage_events** - Stagging table for events
7. **Stage_songs** - Stagging table for songs

The chart below displays this structure:

#### Database Schema
![alt text](https://github.com/phidesigner/DE_nanodegree/blob/master/Project%201/Pics/ERD.png)

## Data sources and repo files
For the purpose of this exercise, the repo only contains information related to the pipeline to process data from the S3 bucket to the Redshift DWH, via python scripts. Information about the data sources can be found here [Song Dataset](http://millionsongdataset.com/) and here [Log Dataset](https://github.com/Interana/eventsim):

#### Prerequisites
- **create_tables.sql** This script needs to be run in Redshift before executing the DAG workflow, so as to create the Tables (artists, songplays, songs, staging_events, staging_songs, time and users)

The project template package used for this project contains three major components for the project:

##### Dags
- **udac_dag.py** The dag template has all the imports and tasks to run Airflow
##### Plugins / Operatos
- **data_quality.py** Operator for data quality checking tasks
- **load_dimensions.py** The operators to read from staging tables and load the dimension tables in Redshift
- **load_fact.py** Operator to load the fact table into Redshift
- **stage_redshift** Operator that reads files from S3 and loads them into Redshift staging tables
##### Helpers
- **sql_queries.py** Redshift statements used in the DAG

## Project Datasets
We work with two datasets that reside in S3. Here are the S3 links for each:

Song data: ```s3://udacity-dend/song_data```
Log data: ```s3://udacity-dend/log_data```

## Required Python libraries
- airflow
- datetime
- timedelta
- os
- pyspark
 
## Running the jobs
You need to install and setup your airflow environment, and config the following connectors:

- asw_credentials (key and password) <- Amazon Services hook
- redshift (server, login, password and port) <- Postgres hook

For this project, task dependencies have been defined in the following manner:
![alt text](https://github.com/phidesigner/DE_nanodegree/blob/master/Project%201/Pics/graph_view.png)

## Test and validation of the output
Two rounds of DAGs were run on this project, in order to check and validate the correctness of the process:
![alt text](https://github.com/phidesigner/DE_nanodegree/blob/master/Project%201/Pics/DAG.png)

With the following timelines, as decribed by the Gantt chart view below:
![alt text](https://github.com/phidesigner/DE_nanodegree/blob/master/Project%201/Pics/Gantt.png)

And the following tree view:
![alt text](https://github.com/phidesigner/DE_nanodegree/blob/master/Project%201/Pics/Tree.png)

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


