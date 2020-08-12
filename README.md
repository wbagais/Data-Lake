# Data Lake Project
The goal of this project is to build an ETL pipeline for a data lake hosted on S3. This project is part of the Data Engineering Udacity Nano degree. 

# Data
There are two databases:
## Song Dataset: 
A subset of dara from <homepage xlink:type="simple" xlink:href="http://millionsongdataset.com/">Million Song Dataset</homepage>. the files are in JSON format.  
## Log Dataset:
The dara from <homepage xlink:type="simple" xlink:href="https://github.com/Interana/eventsim"> event simulator</homepage> based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.The files are also in JSON format. 

## The data are reside in S3. Here are the S3 links for each:
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

# Working Files:
## dl.cfg
Has AWS configuration data (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`)

## etl_testing.ipynb
This file has the same code in the elt.py but saves the result locally. It used for testing the code before deployment. 

## deployment.ipynb
This file copies the etl.py file into the s3 bucket and runs EMR to create a script with the ETL job. 
Running this file will do all the steps. For deployment it uses s3_etl.
- Before running the code in this notebook, run `aws configure` in the command line. 

## etl.py
You can run this file in the command line (`python3 etl.py`).
This file has three functions:
### create_spark_session: 
return the creates  spark session

### process_song_data: 
- read song data and create songs and artists tables. 
- Then partition and parquet the data into S3 bucket

### process_log_data: 
 - read log and song data.
 - create songs and artists tables.
 - Then partition and parquet the data into S3 bucket
 
 ## s3_etl.py
 the same as `etl.py` but without reading AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from the configuration file. 
 This file used instead of `etl.py` when running the code in spark cluster

# Result tables
## Fact Table:
Table name: songplays 
- Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- Songplays table files are partitioned by year and month.
## Dimension Tables
Table name: users
- Columns: user_id, first_name, last_name, gender, level
Table name: songs
- Columns: song_id, title, artist_id, year, duration
- Songs table files are partitioned by year and then artist
Table name: artists
- Columns: artist_id, name, location, latitude, longitude
Table name: time
- Columns: start_time, hour, day, week, month, year, weekday
- Time table files are partitioned by year and month