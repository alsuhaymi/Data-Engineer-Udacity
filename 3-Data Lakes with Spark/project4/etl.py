import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """ process_song_data

        Arguments:
            spark: SparkSession.
            input_data: path to the songs json files.
            output_data: path to the processed songs parquet files.
        """
    
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")
    # extract columns to create songs table
    songs_table = spark.sql("""
                            select distinct songs.song_id, songs.title, 
                            songs.artist_id, songs.year, songs.duration 
                            from songs  
                            where song_id IS NOT NULL """)
    
    # write songs table to parquet files partitioned by year and artis
    #songs_table.write.partitionBy("year", "artist_id").parquet(output_data+'songs/')
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs/')
   
    # extract columns to create artists table
    artists_table = spark.sql("""
                            select distinct songs.artist_id, songs.artist_name, 
                            songs.artist_location, songs.artist_latitude, songs.artist_longitude 
                            from songs  
                            where artist_id IS NOT NULL """)
    # write artists table to parquet files
    #artists_table.write.parquet(output_data+'artists/')
    artists_table.write.mode('overwrite').parquet(output_data+'artists/')

    
def process_log_data(spark, input_data, output_data):
        """ process_song_data

        Arguments:
            spark: SparkSession.
            input_data: path to the logs json files.
            output_data: path to the processed logs parquet files.
        """
        
    # get filepath to log data file
    #log_data = input_data+'log-data/*.json'
    log_data = input_data+'log-data/*.json'
    # read log data file
    df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("logs")
    
    # extract columns for users table    
    users_table = spark.sql("""
                            select distinct logs.firstName, logs.lastName, 
                            logs.gender, logs.level
                            from logs  
                            where userid IS NOT NULL """) 
    
    # write users table to parquet files
    #users_table.write.parquet(output_data+'users/')
    users_table.write.mode('overwrite').parquet(output_data+'users/')

    
    # extract columns to create time table
    time_table = spark.sql(""" select distinct ts as start_time, 
    hour(ts) as hour, 
    dayofmonth(ts) as day, 
    weekofyear(ts) as week, 
    month(ts) as month, 
    year(ts) as year, 
    dayofweek(ts) as weekday 
    from 
    (select to_timestamp(ts/1000) 
    as ts from logs 
    where ts IS NOT NULL ) """)
    
    # write time table to parquet files partitioned by year and month
    #time_table.write.partitionBy("year", "month").parquet(output_data+'time/')
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time/')
    
    songplays_table = spark.sql(""" 
    select distinct monotonically_increasing_id() as songplay_id, 
    to_timestamp(log.ts/1000) as start_time, 
    month(to_timestamp(log.ts/1000)) as month, 
    year(to_timestamp(log.ts/1000)) as year, 
    log.userId as user_id, log.level as level, 
    song.song_id as song_id, song.artist_id as artist_id, 
    log.sessionId as session_id, log.location as location, 
    log.userAgent as user_agent FROM 
    logs log JOIN songs song  on log.artist = song.artist_name and log.song = song.title """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays/')
     

def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    #input_data = "s3a://udacity-dend-suhaymi.s3-us-west-2.amazonaws.com/"
    #output_data = "s3a://udacity-dend-suhaymi.s3-us-west-2.amazonaws.com/"  
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
