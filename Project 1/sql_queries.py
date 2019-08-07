'''
Author: Ivan  Diaz (Based on Udacity DE Nanodegree template
This module creates/drops all tables for the Sparkify database,
and is called from the create_tables.py file
'''


# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES (songplays as Fact table and the remaining four as dimentions:
# users, songs, artists and time)

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id serial PRIMARY KEY, 
    start_time timestamp NOT NULL, 
    user_id numeric NOT NULL, 
    level text NOT NULL, 
    song_id text NOT NULL, 
    artist_id text NOT NULL, 
    session_id int NOT NULL, 
    location text NOT NULL, 
    user_agent text NOT NULL
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id numeric PRIMARY KEY, 
    first_name text NOT NULL, 
    last_name text NOT NULL, 
    gender text NOT NULL, 
    level text NOT NULL
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id text PRIMARY KEY, 
    title text NOT NULL, 
    artist_id text NOT NULL, 
    year int NOT NULL, 
    duration float NOT NULL
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id text PRIMARY KEY, 
    name text NOT NULL, 
    location text, 
    latitude numeric, 
    longitude numeric
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, 
    user_id, 
    level, 
    song_id,
    artist_id, 
    session_id, 
    location, 
    user_agent
    )
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO users (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
    )
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE set level = EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
    )
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, 
    name, 
    location, 
    latitude,
    longitude
    )
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
    )
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS
# Resolves problem with log file not specifying an ID for either the song or the artist
# by getting the song ID and artist ID from the songs and artists tables
# and find matches based on song title, artist name, and song duration time.

song_select = ("""
SELECT s.song_id, a.artist_id 
FROM songs s
JOIN artists a
ON s.artist_id = a.artist_id
WHERE s.title = %s AND a.name = %s AND s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
