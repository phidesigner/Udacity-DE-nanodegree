import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES

IAM_ROLE        = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        event_id            INTEGER IDENTITY(0,1) sortkey distkey,
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER      NOT NULL,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP    NOT NULL,
        userAgent           VARCHAR,
        userId              INTEGER 
);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
            num_songs              INTEGER      NOT NULL,
            artist_id              VARCHAR      NOT NULL distkey sortkey,
            artist_latitude        DECIMAL,
            artist_longitude       DECIMAL,
            artist_location        VARCHAR,
            artist_name            VARCHAR      NOT NULL,
            song_id                VARCHAR      NOT NULL,
            title                  VARCHAR      NOT NULL,
            duration               DECIMAL      NOT NULL,
            year                   INTEGER      NOT NULL
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
            songplay_id     INTEGER     IDENTITY(0,1) PRIMARY KEY distkey sortkey,
            start_time      TIMESTAMP   NOT NULL,
            user_id         INTEGER     NOT NULL,
            level           VARCHAR     NOT NULL,
            song_id         VARCHAR     NOT NULL,
            artist_id       VARCHAR     NOT NULL,
            session_id      INTEGER     NOT NULL,
            location        VARCHAR     NOT NULL,
            user_agent      VARCHAR     NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
            user_id         INTEGER     NOT NULL PRIMARY KEY sortkey,
            first_name      VARCHAR     NOT NULL,
            last_name       VARCHAR     NOT NULL,
            gender          VARCHAR     NOT NULL,
            level           VARCHAR     NOT NULL
    ) diststyle auto;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
                song_id     VARCHAR(50) NOT NULL PRIMARY KEY sortkey,
                title       VARCHAR     NOT NULL,
                artist_id   VARCHAR     NOT NULL,
                year        INTEGER     NOT NULL,
                duration    FLOAT       NOT NULL
    ) diststyle auto;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
            artist_id       VARCHAR     NOT NULL PRIMARY KEY sortkey,
            name            VARCHAR     NOT NULL,
            location        VARCHAR,
            latitude        FLOAT,
            longitude       FLOAT
    ) diststyle auto;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
            start_time      TIMESTAMP       NOT NULL PRIMARY KEY sortkey,
            hour            INTEGER         NOT NULL,
            day             INTEGER         NOT NULL,
            week            INTEGER         NOT NULL,
            month           INTEGER         NOT NULL,
            year            INTEGER         NOT NULL,
            weekday         INTEGER         NOT NULL
    ) diststyle auto;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {}
    REGION 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRIMBLANKS BLANKSASNULL EMPTYASNULL;
    
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    REGION 'us-west-2';
    
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD H H24:MI:SS')AS start_time,
        se.userId                   AS user_id,
        se.level                    AS level,
        ss.song_id                  AS song_id,
        ss.artist_id                AS artist_id,
        se.sessionId                AS session_id,
        se.location                 AS location,
        se.userAgent                AS user_agent
    FROM staging_events se
    JOIN staging_songs ss
    ON se.song = ss.title AND se.artist = ss.artist_name
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        se.userId                   AS user_id,
        se.firstName                AS first_name,
        se.lastName                 AS last_name,
        se.gender                   AS gender,
        se.level                    AS level
    FROM staging_events se
    WHERE se.page = 'NextSong' AND se.userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        ss.song_id                  AS song_id,
        ss.title                    AS title,
        ss.artist_id                AS artist_id,
        ss.year                     AS year,
        ss.duration                 AS duration
    FROM staging_songs ss;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        ss.artist_id                AS artist_id,
        ss.artist_name              AS name,
        ss.artist_location          AS location,
        ss.artist_latitude          AS latitude,
        ss.artist_longitude         AS longitude
    FROM staging_songs ss;
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD H H24:MI:SS')AS start_time,
        EXTRACT(hour FROM start_time)    AS hour,
        EXTRACT(day FROM start_time)     AS day,
        EXTRACT(week FROM start_time)    AS week,
        EXTRACT(month FROM start_time)   AS month,
        EXTRACT(year FROM start_time)    AS year,
        EXTRACT(weekday FROM start_time) AS weekday
    FROM staging_events se
    WHERE se.page = 'NextSong';
""")

# GET NUMBER OF ROWS IN EACH TABLE
get_number_staging_events = ("""
    SELECT COUNT(*) FROM staging_events
""")

get_number_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

get_number_songplays = ("""
    SELECT COUNT(*) FROM songplays
""")

get_number_users = ("""
    SELECT COUNT(*) FROM users
""")

get_number_songs = ("""
    SELECT COUNT(*) FROM songs
""")

get_number_artists = ("""
    SELECT COUNT(*) FROM artists
""")

get_number_time = ("""
    SELECT COUNT(*) FROM time
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
select_number_rows_queries= [get_number_staging_events, get_number_staging_songs, get_number_songplays, get_number_users, get_number_songs, get_number_artists, get_number_time]

