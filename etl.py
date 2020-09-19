import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, TimestampType, DateType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')\
                    .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    print('write songs')
    songs_table = songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')\
                      .withColumnRenamed('artist_name', 'name')\
                      .withColumnRenamed('artist_location', 'location')\
                      .withColumnRenamed('artist_latitude', 'latitude')\
                      .withColumnRenamed('artist_longitude', 'longitude')\
                      .dropDuplicates()
    
    # write artists table to parquet files
    print('write artists')
    artists_table = artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')\
                    .dropDuplicates()
    
    # write users table to parquet files
    print('write users')
    users_table = users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # filter song play action only
    action_df = df.filter(df.page == 'NextSong')\
                  .select('ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent') 
    
    # create timestamp, datetime columns from original ts column
    def format_datetime(ts):
        return datetime.fromtimestamp(ts/1000.0)
    
    get_timestamp = udf(lambda x: format_datetime(int(x)),TimestampType())
    get_datetime = udf(lambda x: format_datetime(int(x)), DateType())
    
    action_df = action_df.withColumn('timestamp', get_timestamp(action_df.ts))\
                         .withColumn('datetime', get_datetime(action_df.ts))
    
    
    # extract columns to create time table
    tsdf = action_df.select('timestamp').dropDuplicates()
    time_table = tsdf.withColumn('start_time', tsdf.timestamp)\
                      .withColumn('hour',  hour(tsdf.timestamp))\
                      .withColumn('day',   dayofmonth(tsdf.timestamp))\
                      .withColumn('week',  weekofyear(tsdf.timestamp))\
                      .withColumn('month', month(tsdf.timestamp))\
                      .withColumn('year',  year(tsdf.timestamp))\
                      .withColumn('weekday', dayofweek(tsdf.timestamp))\
                      .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    print('write time')
    time_table = time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'), 'overwrite')

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json') 
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView('song_df_table')
    action_df.createOrReplaceTempView('action_df_table')
    songplays_table = spark.sql('''
                               Select row_number() over(order by a.timestamp) as songplay_id,
                                      a.timestamp as start_time, 
                                      a.userID as user_id,
                                      a.level,
                                      s.song_id,
                                      s.artist_id,
                                      a.sessionId as session_id,
                                      a.location,
                                      a.userAgent as user_agent
                               from song_df_table s inner join action_df_table a
                                   on s.title=a.song and a.artist=s.artist_name
                            ''')
    songplays_table =songplays_table.withColumn('songplay_id', monotonically_increasing_id())
    # write songplays table to parquet files partitioned by year and month
    print('write songplays')
    songplays_table = songplays_table.withColumn("year",year('start_time')).withColumn("month",month('start_time')).write.partitionBy("year","month").parquet(os.path.join(output_data, 'songplays/'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify0823/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
