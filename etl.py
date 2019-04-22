import glob

from cassandra.cluster import Cluster
import pandas as pd
import numpy as np

from sql_statements import insert_music_library, insert_artist_library, \
    insert_user_library, select_music_library, select_artist_library, select_queries


def main():
    try:
        session, cluster = connect_to_database()

        # write data
        create_csv_from_source()
        df = pd.read_csv('./event_datafile_new.csv')

        # perform inserts
        insert_into_music_library(session, df)
        insert_into_artist_library(session, df)
        insert_into_user_library(session, df)

        # perform selects
        perform_select_queries(session)

    finally:
        session.shutdown()
        cluster.shutdown()


def connect_to_database():
    """
    Connects to Cassandra database
    :return: session object
    """
    try:
        cluster = Cluster()
        session = cluster.connect()
        session.set_keyspace('sparkifydb')
        return session, cluster
    except Exception as e:
        print('Error connecting to database')
        print(e)


def create_csv_from_source():
    """
    Concatenates all csv files, loads  data into a dataframe and writes to event_datafile_new.csv
    :return: dataframe containing all the rows
    """
    path = './event_data/'
    csv_files = glob.glob(path + "/*.csv")
    df = pd.concat((pd.read_csv(f) for f in csv_files))
    df.to_csv('./event_datafile_new.csv')


def insert_into_music_library(session, df):
    data = df[['sessionId', 'itemInSession', 'artist', 'song', 'length']]

    # replace empty string with None in decimal type column
    cleaned_data = data.replace({pd.np.nan: None}).values.tolist()

    query = insert_music_library

    try:
        for entry in cleaned_data:
            session.execute(query, entry)
    except Exception as e:
        print('Error inserting data into table music_library:')
        print(e)


def insert_into_artist_library(session, df):
    data = df[['artist', 'song', 'itemInSession', 'firstName', 'lastName', 'userId', 'sessionId']]
    data[['userId']] = data[['userId']].fillna(-1).astype(np.int)

    cleaned_data = data.replace({pd.np.nan: None}).values.tolist()
    query = insert_artist_library

    try:
        for entry in cleaned_data:
            session.execute(query, entry)
    except Exception as e:
        print('Error inserting data into table artist_library:')
        print(e)


def insert_into_user_library(session, df):
    data = df[['userId', 'sessionId', 'firstName', 'lastName', 'song']]
    data[['userId']] = data[['userId']].fillna(-1).astype(np.int)

    cleaned_data = data.replace({pd.np.nan: None}).values.tolist()
    query = insert_user_library

    try:
        for entry in cleaned_data:
            # since song is part of PK, it cannot be null - skip if so
            if entry[4] == None:
                continue
            session.execute(query, entry)
    except Exception as e:
        print('Error inserting data into table user_library:')
        print(e)


def perform_select_queries(session):
    query_description = (
    "1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4",
    "\n2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182",
    "\n3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'",
    )
    index = 0

    for select_query in select_queries:
        query = session.execute(select_query)
        print(query_description[index])
        index += 1

        for row in query:
            print(row)


main()