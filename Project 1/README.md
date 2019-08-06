# Project: Data Modeling with Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. The data resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Purpose
This project aims at creating a Postgres database with tables designed to optimize queries on song play analysis, as well as the ETL pipeline for future analysis.

For this a predifined ETL process is executed and star schema is chosen as the more flexible and convenient approach to the analytical requirements. The fact table is defined as the songplay list and four dimension tables are created with information about:

- Time
- User
- Song
- Artists

The chart below displays this structure:

#### Database Schema
![alt text](https://github.com/phidesigner/DE_nanodegree/blob/master/Project%201/Pics/ERD.png)

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


