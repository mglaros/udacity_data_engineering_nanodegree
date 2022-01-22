import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS", "AWS_SECRET_ACCESS_KEY")

def create_spark_session():
    """
    Description:
        Create and return SparkSession object for downstream jobs to utilize
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Description:
        Process the songs data files and create extract songs table and artist table data from it.
    :param spark: a spark session instance
    :param input_data: input S3 file path
    :param output_data: output S3 file path
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    # read song data file
    df = spark.read.json(song_data)
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + 'songs/')
    

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude")
    
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists/')
    
    
def process_log_data(spark, input_data, output_data):
    """
    Description:
            Process the event log file and extract data for table time, users and songplays from it.
    :param spark: a spark session instance
    :param input_data: input S3 file path
    :param output_data: output S3 file path
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level")
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users/')
    

    # create timestamp scolumn from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(F.col("ts")))

    
    # extract columns to create time table
    time_table = df.selectExpr("timestamp as start_time")\
        .withColumn("hour", F.hour(F.col("start_time")))\
        .withColumn("day", F.dayofmonth(F.col("start_time")))\
        .withColumn("week", F.weekofyear(F.col("start_time")))\
        .withColumn("month", F.month(F.col("start_time")))\
        .withColumn("year", F.year(F.col("start_time")))\
        .withColumn("weekday", F.dayofweek(F.col("start_time")))
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + 'time/')

    # read in song data to use for songplays table
    #rename year column to avoid ambiguous selection joining to time_table downstream
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json').withColumnRenamed("year", "song_year")

    #create unique songplay_id
    window_spec = Window.orderBy(F.lit('A'))
    # extract columns from joined song and log datasets to create songplays table 
    #join to timetable to get year and month partition information 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name), 'inner')\
        .join(time_table, df.timestamp == time_table.start_time, 'inner')\
        .withColumn("songplay_id", F.row_number().over(window_spec))\
        .selectExpr("songplay_id", "start_time", "userId as user_id", "level", "song_id", "artist_id", "sessionId as session_id", "location", "userAgent as user_agent", "year", "month")
        

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + 'songplays/')



def main():
    """
    Description:
        driver program that processes song and log data
        into dimension and fact tables that are written
        to the S3 output_data path
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-mglaros-data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
