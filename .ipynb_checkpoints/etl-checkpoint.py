import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


# Read in AWS configuration
config = configparser.ConfigParser()
config.read('dl.cfg')

# Set AWS configuration as environment variables
os.environ['AWS_ACCESS_KEY_ID']=config['S3']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['S3']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create Spark session
    
    Parameters: 
    None
  
    Returns: 
    spark: Spark session object
    
    """

    # Create Spark session with the AWS Hadoop jar
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the song files
    
    Parameters:
    spark: Spark session object
    input_data: S3 data path for input files
    output_data: S3 data path for output files
    
    Returns:
    None
    
    """
    
    # Get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"])
        
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("append").partitionBy("year").parquet(output_data + 'song_table.parquet')
    
    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])
    
    # write artists table to parquet files
    artists_table.write.mode("append").parquet(output_data + 'artist_table.parquet')


def process_log_data(spark, input_data, output_data):
    """
    Process the log files
    
    Parameters:
    spark: Spark session object
    input_data: S3 data path for input files
    output_data: S3 data path for output files
    
    Returns:
    None
    
    """
        
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    
    # read log data file
    df = spark.read.json(log_data)
        
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    
    # extract columns for users table
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"])

    # write users table to parquet files
    users_table.write.mode("append").parquet(output_data + 'users_table.parquet')
    
    # create hour column from datetime
    get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).hour)
    df = df.withColumn("hour", get_hour(df.ts))

    # create day column from datetime
    get_day = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).day)
    df = df.withColumn("day", get_day(df.ts))
    
    # create week column from datetime
    get_week = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).isocalendar()[1])
    df = df.withColumn("week", get_week(df.ts))
        
    # create month column from datetime
    get_month = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).month)
    df = df.withColumn("month", get_month(df.ts))
    
    # create year column from datetime
    get_year = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).year)
    df = df.withColumn("year", get_year(df.ts))

    # create weekday column from datetime
    get_weekday = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).weekday())
    df = df.withColumn("weekday", get_weekday(df.ts))
            
    # extract columns to create time table
    time_table = df.select(["ts", "hour", "day", "week", "month", "year", "weekday"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("append").partitionBy("year", "month").parquet(output_data + "time_data.parquet")
            
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'song_table.parquet')

    # extract columns from joined song and log datasets to create songplays table
    df.createOrReplaceTempView("log_view")
    song_df.createOrReplaceTempView("song_view")
    songplays_table = spark.sql('''
    SELECT log_view.ts as start_time,
        log_view.userId as user_id,
        log_view.level as level,
        song_view.song_id as song_id,
        song_view.artist_id as artist_id,
        log_view.sessionId as session_id,
        log_view.location as location,
        log_view.userAgent as user_agent,
        log_view.year as year,
        log_view.month as month
    FROM log_view
    JOIN song_view ON (log_view.song = song_view.title)
    ''')
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("append").partitionBy("year", "month").parquet(output_data + 'songplays_table.parquet')


def main():
    """
    Main function
    
    Parameters: 
    None
  
    Returns: 
    None
    
    """
    
    # Create Spark session
    spark = create_spark_session()
    
    # Define input and output paths
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://proj4output/"
    
    # Process the song data
    process_song_data(spark, input_data, output_data)
    
    # Process the log data
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
