import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function populates the songs and artists dimension tables after 
    selecting specific fields from each of the log files found under data/song_data
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    artist_select_cols = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    song_select_cols = ["song_id", "title", "artist_id", "year", "duration"]
    
    # insert artist record
    artist_data = list(df[artist_select_cols].values[0])
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    song_data = list(df[song_select_cols].values[0])
    cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath):
    """
    This function populates the time and users dimension tables
    and the songplays fact table from each of the files under data/log_data
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
   
    #initialize the data to pass to the pd.DataFrame() constructor 
    #will be of the form {'start_time': pd.Series, 'hour': pd.Series, ...}
    data_dict = dict(list(zip(column_labels, time_data)))
    time_df = pd.DataFrame(data_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data =  (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function locates all of the files found under filepath, which can
    be either data/song_data or data/log_data and calls process_song_data or process_log_data
    respectively for each of the files found
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """driver function for the entire ETL pipeline"""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()