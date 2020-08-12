from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
import pyspark.sql.functions as F
import datetime
import os
import configparser


def create_spark_session():
    """
    create spark session
    ----------
    return: spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    read song data and create songs and artists tables.
    Then partition and parquet the data into S3 bucket
    ----------
    ARGS
    spark: session 
           created in create_spark_session function
    input_data: path
           The path for udacity-dend s3 bucket which contains the song data.
    output_data: path
            The path for the parquet files.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    song_df = spark.read.json(song_data) 
    song_df.createOrReplaceTempView("song_df")

    # extract columns to create songs table
    songs_table = spark.sql("""
                SELECT DISTINCT 
                row_number() over (ORDER BY year,artist_id)  id,
                song_id, 
                title, 
                artist_id, 
                year, 
                duration
                FROM song_df
    """)
    print("songs_table created")
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/', mode = "overwrite")
    print("songs_data saved")

    # extract columns to create artists table
    artists_table = spark.sql("""
                SELECT DISTINCT 
                artist_id, 
                artist_name as name, 
                artist_location as location, 
                artist_latitude as lattitude, 
                artist_longitude as longitude
                FROM song_df
    """)
    artists_table =artists_table.withColumn("id",  F.monotonically_increasing_id())
    
    print("artists_table created")
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/', mode = "overwrite")
    print("artists_data saved")


def process_log_data(spark, input_data, output_data):
    """
    read log and song data.
    create songs and artists tables.
    Then partition and parquet the data into S3 bucket
    ----------
    ARGS
    spark: session 
           created in create_spark_session function
    input_data: path
           The path for udacity-dend s3 bucket which contains the song data.
    output_data: path
            The path for the parquet files.
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json' 

    # read log data file
    log_df = spark.read.json(log_data)   
    
    # filter by actions for song plays
    log_df = log_df.where("page = 'NextSong'")
    
    log_df.createOrReplaceTempView("log_df") 
    
    # extract columns for users table    
    users_table = spark.sql("""
            SELECT DISTINCT
            userId    as user_id,
            firstName as first_name,
            lastName  as last_name,
            gender    as gender,
            level
            from log_df           
    """)
    
    print("users_table created")
    users_table =users_table.withColumn("id",  F.monotonically_increasing_id())
    
    # write users table to parquet files
    users_table.write.parquet( output_data + 'users/', mode = "overwrite")
    print("users_table saved")
    
    # create datetime function from original timestamp column
    spark.udf.register("date_time", lambda x: str(datetime.datetime.fromtimestamp(x / 1000.0)))
    
    # extract columns to create time table
    time_table = spark.sql('''
                  SELECT DISTINCT 
                  date_time,
                  date_time AS start_time,
                  hour(date_time) AS hour,
                  dayofmonth(date_time) AS day,
                  weekofyear(date_time) AS week,
                  month(date_time) AS month,
                  year(date_time) AS year,
                  dayofweek(date_time) AS weekday
                  FROM (SELECT date_time(ts) AS date_time FROM log_df) 
                  ORDER BY year, month
          '''
          )
    print("time_table created")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/', mode = "overwrite")
    print("time_table saved")

    # read in song data to use for songplays table
    song_df = spark.read.json( input_data + "song_data/*/*/*/*.json")
    song_df.createOrReplaceTempView("song_df")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT DISTINCT
        date_time(ts)        as start_time,
        year(date_time(ts))  as year,
        month(date_time(ts)) as month,
        l.userId             as user_id,
        l.level              as level,
        s.song_id            as song_id,
        s.artist_id          as artist_id,
        l.sessionId          as session_id,
        l.location           as location,
        l.userAgent          as user_agent
        FROM log_df   AS l
        JOIN song_df  AS s
        ON (l.artist = s.artist_name
        AND l.song   = s.title)  
    """)
    print("songplays_table created")
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/', mode = "overwrite")
    print("songplays_table saved")


def main():
    """
    Call create_spark_session, process_song_data, and process_log_data functions
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://wejdan-udacity-dend/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
