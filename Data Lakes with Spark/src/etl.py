import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function is to read the song data in the filepath (bucket/song_data)
    to get the song and artist info. 
    
    Args:
    ----------------------------------------
        spark:        the cursor object
        input_data:   the path of the bucket containing song data
        output_data:  the path where the parquet files stored
    
    Return:
        None
    """
    # get filepath to song data file
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    
    # read song data file    
    song_data = spark.read.json(song_data, sep=";", inferSchema=True, header=True)
    
    # song_data.printSchema()
    print('Success of reading song_data from S3.')

    # extract columns to create songs table
    # songs table: song_id, title, artist_id, year, duration
    songs_table = song_data.select('song_id', 'title', 'artist_id', 'year',
                                  'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f'{output_data}/songs_table', mode='overwrite',
                             partitionBy=['year', 'artist_id'])
    
    # check on songs_table
    print("Success of writing sons_table to parquet")

    # extract columns to create artists table
    # artist table: artist_id, name, location, latitude, longitude
    artists_table = song_data.select('artist_id', 'artist_name', 'artist_location',
                                    'artist_latitude', 'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}/artists_table', mode='overwrite')
    
    # check on artists_table
    print("Success of writing artists_table to parquet")

def process_log_data(spark, input_data, output_data):
    """
    This function is to read the log data in the filepath (bucket/log_data)
    to get the info. to populate the users, time and song tables.
    
    Args:
    ------------------------------
        spark:       the cursor object
        input_data:  the path to the bucket containing song data
        output_data: the path where the parquet files stored
        
    Returns:
        None
    
    """
    
    # get filepath to log data file
    log_data = f'{input_data}/log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    print("Success of reading log_data from S3")
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table  
    # users table: user_id, first_name, last_name, gender, level
    user_table = df.select('userId', 'firstName', 'lastName',
                             'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    user_table.write.parquet(f'{output_data}/user_table', mode='overwrite')
    print('Success of writing user_table to parquet')
    
    # convert ts column to timestamp
    df = df.withColumn('start_time', F.from_unixtime(F.cols('ts')/1000))

    # extract columns to create time table
    # time table: start_time, hour, day, week, month, year, weekday
    time_table = df.select('ts', 'start_time') \
                   .withColumn('year', F.year('start_time')) \
                   .withColumn('month', F.month('start_time')) \
                   .withColumn('week', F.weekofyear('start_time')) \
                   .withColumn('weekday', F.dayofweek('start_time')) \
                   .withColumn('day', F.dayofyear('start_time')) \
                   .withColumn('hour', F.hour('start_time')).dropDuplicates()
    print('Success of extracting time column')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(f'{output_table}/time_table', mode='overwrite',
                             partitionBy=['year', 'month']
                            )
    print('Success of writing time_table to parquet')
    
    # read in song data to use for songplays table
    song_data = f'{input_data}/song_data/A/A/A/*.json'
    song_dataset = spark.read.json(song_data)
    print('Success of reading song_dataset from S3')
    
    # create temporary view of song_dataset, time_table and log_dataset 
    song_dataset.createOrReplaceTempView('song_dataset')
    time_table.createOrReplaceTempView('time_table')
    df.createOrReplaceTempView('log_dataset')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT DISTINCT
                                   FROM song_dataset s
                                   JOIN log_dataset l
                                   ON s.title = l.song
                                       AND s.duration = l.length
                                       AND s.artist_name  = l.artist
                                       JOIN time_table t
                                   ON t.ts = l.ts"""
                                ).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f'{output_data}/songplays_table', mode='overwrite',
                                 partitionBy=['year', 'month'])

    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://spark-bucket"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
