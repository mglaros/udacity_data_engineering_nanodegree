from cluster_helpers import load_config


# CONFIG
config = load_config()

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""

CREATE TABLE IF NOT EXISTS staging_events

(

    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    iteminSession INTEGER,
    lastName VARCHAR,
    length DECIMAL,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INTEGER

);

""")


staging_songs_table_create = ("""

CREATE TABLE IF NOT EXISTS staging_songs

(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude DECIMAL,
    artist_longitude DECIMAL,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration DECIMAL,
    year INTEGER

);

""")

songplay_table_create = ("""

CREATE TABLE IF NOT EXISTS songplays

(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP REFERENCES time(start_time) SORTKEY,
    user_id INT REFERENCES users(user_id),
    level VARCHAR,
    song_id VARCHAR REFERENCES songs(song_id), 
    artist_id VARCHAR REFERENCES artists(artist_id),
    session_id INT,
    location VARCHAR, 
    user_agent VARCHAR
    
);

""")

user_table_create = ("""

CREATE TABLE IF NOT EXISTS users

(
     user_id INT PRIMARY KEY,
     first_name VARCHAR, 
     last_name VARCHAR, 
     gender CHAR(1), 
     level VARCHAR

)
DISTSTYLE ALL;

""")

song_table_create = ("""

CREATE TABLE IF NOT EXISTS songs

(
    song_id VARCHAR PRIMARY KEY SORTKEY, 
    title VARCHAR, 
    artist_id VARCHAR REFERENCES artists(artist_id), 
    year INT, 
    duration DECIMAL


)
DISTSTYLE ALL;

""")

artist_table_create = ("""

CREATE TABLE IF NOT EXISTS artists

(
    artist_id VARCHAR PRIMARY KEY SORTKEY,
    name VARCHAR, 
    location VARCHAR, 
    latitude DECIMAL, 
    longitude DECIMAL

)
DISTSTYLE ALL;

""")

time_table_create = ("""

CREATE TABLE IF NOT EXISTS time

(
    start_time TIMESTAMP PRIMARY KEY SORTKEY,
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT

)
DISTSTYLE ALL;

""")

# STAGING TABLES

staging_events_copy = ("""

    COPY staging_events 
        FROM '{}' 
        CREDENTIALS 'aws_iam_role={}'
        REGION 'us-west-2'
        FORMAT AS JSON '{}'
        TIMEFORMAT 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;

""").format(config["LOG_DATA"], config["IAM_ROLE"], config["LOG_JSONPATH"] )

staging_songs_copy = ("""

    COPY staging_songs 
        FROM '{}' 
        CREDENTIALS 'aws_iam_role={}'
        REGION 'us-west-2'
        FORMAT AS JSON 'auto'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
    

""").format(config["SONG_DATA"], config["IAM_ROLE"])

# FINAL TABLES



songplay_table_insert = ("""

    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        se.ts AS start_time,
        se.userId AS user_id,
        se.level AS level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        se.sessionId AS session_id,
        se.location AS location,
        se.userAgent AS userAgent
    FROM staging_events se
    LEFT JOIN staging_songs ss
    ON se.artist = ss.artist_name
    AND se.song = ss.title
    WHERE se.page = 'NextSong';

""")


user_table_insert = ("""

    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender AS gender,
        level AS level
    FROM staging_events
    WHERE userId IS NOT NULL;
    
""")


song_table_insert = ("""

    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id AS song_id,
        title AS title,
        artist_id AS artist_id,
        year AS year,
        duration AS duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;

""")



artist_table_insert = ("""

    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
    
""")

time_table_insert = ("""

    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        se.ts AS start_time,
        EXTRACT(HOUR FROM se.ts) AS hour,
        EXTRACT(DAY FROM se.ts) AS day,
        EXTRACT(WEEK FROM se.ts) AS week,
        EXTRACT(MONTH FROM se.ts) AS month,
        EXTRACT(YEAR FROM se.ts) AS year,
        EXTRACT(WEEKDAY FROM se.ts) AS weekday
    FROM staging_events se
    WHERE ts IS NOT NULL
    AND se.page = 'NextSong';

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,  user_table_create, artist_table_create, time_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [time_table_insert, artist_table_insert, user_table_insert, song_table_insert,songplay_table_insert]
