# Data Lake

## Purpose

A music streaming startup wants to move their data warehouse to a data lake. The purpose of this project is to build an ETL pipeline that extracts data from S3, processes the data in Spark and then loads the results back into S3 as a set of dimensional tables which will be used by their analytics team to gain insight into user behavior. 

## Dataset

The first [dataset](s3://udacity-dend/song_data) contains the song data in JSON format. Files are partitioned by first three letters of each song's track ID e.g. `song_data/A/B/C/TRABCEI128F424C983.json`. Example of a single song file:
```
{
    "num_songs": 1, 
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null, 
    "artist_longitude": null, 
    "artist_location": "", 
    "artist_name": "Line Renaud", 
    "song_id": "SOUPIRU12A6D4FA1E1", 
    "title": "Der Kleine Dompfaff", 
    "duration": 152.92036, 
    "year": 0
}
```

The second [dataset](s3://udacity-dend/log_data) contains the log metadata. The log files are paratitioned by year and month e.g., `log_data/2018/11/2018-11-12-events.json`. Example entry in a log file:
```
{
    "artist": "Pavement",
    "auth": "Logged in",
    "firstName": "Sylvie",
    "gender": "F",
    "iteminSession": 0,
    "lastName": "Cruz",
    "length": 99.16036,
    "level": "free",
    "location": "Kiamath Falls, OR",
    "method": "PUT",
    "page": "NextSong",
    "registration": 1.540266e+12,
    "sessionId": 345,
    "song": "Mercy: The Laundromat",
    "status": 200,
    "ts": 1541990258796,
    "userAgent": "Mozzilla/5.0...",
    "userId": 10
}
```

## Schema for Song Play Analysis

We will create a star schema using the song and log datasets, which is optimized for queries on song play analysis. The star schema contains the following fact and dimension tables:
1. `songplay` (fact) - records event data for each song play
    + `songplay_id`
    + `start_time`
    + `user_id`
    + `level`
    + `song_id`
    + `artist_id`
    + `session_id`
    + `location`
    + `user_agent`
2. `users` (dimension) - users in the app
    + `user_id`
    + `first_name`
    + `last_name`
    + `gender`
    + `level`
3. `song` (dimension) - songs in music database
    + `song_id`
    + `title`
    + `artist_id`
    + `year`
    + `duration`
4. `artist` (dimension) - artists in music database
    + `artist_id`
    + `name`
    + `location`
    + `lattitude`
    + `longitude`
5. `time` (dimension) - timestamps of records in `songplays`
    + `start_time`
    + `hour`
    + `day`
    + `week`
    + `month`
    + `year`
    + `weekday`
    
 ## ETL Pipeline
 
 To execute the ETL pipeline run `python etl.py`. This will perform the following:
 
 1. Create a spark session object
 2. Read song data from s3 into Spark dataframe
 3. Create `songs` and `artists` tables 
 4. Write `songs` and `artists` tables into parquet files and store into s3
 5. Read log data from s3 into Spark dataframe
 6. Create `users` and `times` tables
 7. Write `users` and `times` tables into parquet files and store into s3
 8. Create `songplays` table by joining log and song data 
 9. Write `songplays` table into parquet file and store in s3