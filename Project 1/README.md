# Project: Data Modeling with Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. The data resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Purpose
This project aims at creating a Postgres database with tables designed to optimize queries on song play analysis, as well as the ETL pipeline for future analysis.

For this a predifined ETL process is executed and star schema is chosen as the more flexible and convenient approach to the analytical requirements e.g. (DWH Fundamentals: A comprehensive guide for IT professionals)

- Easy for users to understand
- Optimizes navigation
- Most suitable for query processing
- Enables specific performance schemes

The fact table is defined as the songplay list and four dimension tables are created with information about:

- Time
- User
- Song
- Artists

The chart below displays this structure:

#### Database Schema
![alt text](https://github.com/phidesigner/DE_nanodegree/blob/master/Project%201/Pics/ERD.png)

## Data sources and repo files
For the purpose of this exercise, the repo only contains information related to the ETL and Postgres processes. Information about the data sources can be found here [Song Dataset](http://millionsongdataset.com/) and here [Log Dataset](https://github.com/Interana/eventsim):

- **sql_queries.py:** defines the schema of the five tables and the corresponding SQL queries
- **create_tables.py:** triggers the creation of the Database and the five afore mentioned tables
- **elt.py:** runs the comeplete ETL pipeline
- **etl.ipynb:** given instructions notebook to develop the ETL pipeline
- **test.ipynb:** given notebook to test the sql queries and ETL outcome

## Required Python libraries
- os
- glob
- pandas
- psycopg2
- sql_queries
 
## Running the jobs
```
$ python create_tables.py
$ python etl.py
```
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


