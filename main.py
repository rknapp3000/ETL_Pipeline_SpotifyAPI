import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE= "sqlite:///my_played_tracks.sqlite"
USER_ID = "1275336670"
TOKEN = "BQCUui70elfwZHFZ3YUOTo-bnkzkUomxyJHEyOnZHqI_E5ern-0zRusHMMqNX_xeAAyMyegThlJMB7u0w4_nYVZjE7skVkfOjfPWIvDB3nN_5zpudOD46crvRJS4rgK1eGKMg-t_vPqgxnkgZWsj"

def validate_data(df: pd.DataFrame) -> bool:
    # Check that there is data in the dataframe seeing if its empty
    if df.empty:
        print("No songs were downloaded. Finishing executing the program")
        return False

    # Checking if there is a primary key
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Violation of the primary key check")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values were found in data")

    #  Check that all timestamps are of yesterday's date, commented out to use data from current day
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    #
    # timestamps = df["timestamp"].tolist()
    # for timestamp in timestamps:
    #    # if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
    #     #    raise Exception("At least one of the returned songs does not have a yesterday's timestamp")
    #
    #     return True

if __name__ == "__main__":
    # Extracting the data part of the ETL process

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    # Converting the time to Unix timestamp in miliseconds specified by API
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours
    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers=headers)

    data = r.json()

    songs = []
    artist = []
    played_at = []
    timestamps = []

    # Extracting only the relevant fields of data from the JSON file
    for song in data["items"]:
        songs.append(song["track"]["name"])
        artist.append(song["track"]["album"]["artists"][0]["name"])
        played_at.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    # Create a dictionary to turn into Pandas dataframe below
    song_dict = {
        "song_name": songs,
        "artist_name": artist,
        "played_at": played_at,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])

    # Validate the data using the method created above, if the data is valid then proceed to loading stage
    if validate_data(song_df):
        print("Data valid, proceed to load stage")

    # Load the data to the database

    engine = sqlalchemy.create_engine(DATABASE)
    connection = sqlite3.connect('my_played_tracks.sqlite')
    cursor = connection.cursor()

    sql_query = """
       CREATE TABLE IF NOT EXISTS my_played_tracks(
           song_name VARCHAR(200),
           artist_name VARCHAR(200),
           played_at VARCHAR(200),
           timestamp VARCHAR(200),
           CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
       )
       """

    cursor.execute(sql_query)
    print("Database opened successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    connection.close()
    print("Database closed successfully")



