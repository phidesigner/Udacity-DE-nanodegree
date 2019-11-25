import configparser
from datetime import datetime
import os
from pyspark.sql.functions import udf, col
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp, monotonically_increasing_id
from pyspark.sql.types import TimestampType

# Accessing AWS credentials (dl.cfg)
config = configparser.ConfigParser() 
config.read(os.path.expanduser("~/.aws/credentials"))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
        Initiating the spark/hadoop session on AWS
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    print('Ok spark session')
    return spark

def process_song_data(spark, input_data, output_data):
    '''
        Processes song_data files from an S3, extracting the songs and artist tables. Outputs a compressed parquet file for each.
        spark      : Spark session
        input_data : S3 path for song_data
        output_data: S3 bucket path were tables will be stored in parquet format
    '''
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        'song_id',
        'title',
        'artist_id', 
        'year', 
        'duration'
    ).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs.parquet')

    # extract columns to create artists table
    artists_table = df.select(
        'artist_id',
        col('artist_name').alias('name'),
        col('artist_location').alias('location'),
        col('artist_latitude').alias('latitude'),
        col('artist_longitude').alias('longitude')
    ).distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists.parquet')
    print('Ok processing song_data')


def process_log_data(spark, input_data, output_data):
    '''
        Processes log_data files from an S3, extracting the user, time and songplays tables. Outputs a compressed parquet file for each table.
        spark      : Spark session
        input_data : S3 path for log_data
        output_data: S3 bucket path were tables will be stored in parquet format
    '''
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    # log_data = input_data + 'log_data/'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong').filter(df.userId.isNotNull())

    # extract columns for users table    
    users_table = df.select(
        col('userId').alias('user_id'),
        col('firstName').alias('first_name'),
        col('lastName').alias('last_name'),
        'gender',
        'level').distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: str(int(int(ts)/1000)))
    df = df.withColumn('timestamp', get_timestamp(col('ts')))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda dt: str(datetime.fromtimestamp(int(dt) / 1000)))
    df = df.withColumn('datetime', get_datetime(col('ts')))
    
    # extract columns to create time table
    time_table = df.select(
        'timestamp',
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        date_format('datetime', 'E').alias('weekday')
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs.parquet')

    # extract columns from joined song and log datasets to create songplays table 
    ts_Format = 'yyyy/MM/dd HH:MM:ss z'
    songplays_table = song_df.join(df, song_df.artist_id == df.artist)\
    .withColumn('songplay_id', monotonically_increasing_id())\
    .withColumn('start_time', to_timestamp(date_format((col('ts') / 1000)\
                                                       .cast(dataType = TimestampType()), ts_Format), ts_Format))\
    .select(
        'songplay_id',
        'start_time',
        'level',
        'song_id',
        'artist_id',
        'userAgent',
        'location',
        col('userId').alias('user_id'),
        col('sessionId').alias('session_id'),
        month(col('start_time')).alias('month'),
        year(col('start_time')).alias('year')
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays.parquet')
    print('Ok processing log_data')


def main():
    '''
        Main function to create the spark session, and run functions to proccess song_data and log_data. It also contains the paths to retreive and export dimensional tables to and from the S3. 
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    print('Ok everything')

if __name__ == "__main__":
    main()
