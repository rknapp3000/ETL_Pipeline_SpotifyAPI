import requests
import logging
import sqlalchemy
import sqlite3
from pandas import DataFrame
from datetime import datetime
from typing import Dict


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

DATABASE= "sqlite:///most_played_artists.sqlite"


def get_user_data(access_token: str) -> Dict:
    """Function to get raw user data from api using access token"""
    
    user_data_url = 'https://api.spotify.com/v1/me/top/artists'

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    # Make a GET request to fetch user's data
    response = requests.get(user_data_url, headers=headers)

    # If the request is successful, return the JSON data
    if response.status_code == 200:
        return response.json()  # This will return the user data as a JSON response
    else:
        return {"error": f"Error fetching user data: {response.status_code} - {response.text}"}
    

def transform_data(user_data: Dict) -> DataFrame:
    """Transforms the raw user data and picks fields out of the json that we want in our final table"""
    current_date = datetime.now().strftime('%Y-%m-%d')

    artists = []
    
    for artist in user_data['items']:
        artist_info = {
            "artist_name": artist.get('name', ''),
            "popularity_rating": artist.get('popularity', 0),
            "artist_id": artist.get('id', ''),
            "spotify_url": artist['external_urls'].get('spotify', ''),
            "genres": artist.get('genres', []),
            "followers_count": artist['followers'].get('total', 0),
            "images": [image['url'] for image in artist.get('images', [])],
            "current_date": current_date
        }
        artists.append(artist_info)

    df = DataFrame(artists)

    return df


def load_data(df: DataFrame) -> None: 
    """creates table in database and inserts data into table."""
    
    # Preprocess the DataFrame to convert lists to strings (JSON or comma-separated)
    df['genres'] = df['genres'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    df['images'] = df['images'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
        
    engine = sqlalchemy.create_engine(DATABASE)
    connection = sqlite3.connect('most_played_artists.sqlite')
    cursor = connection.cursor()

    sql_query = """
       CREATE TABLE IF NOT EXISTS most_played_artists(
            artist_name TEXT,
            popularity_rating INTEGER,
            artist_id TEXT,
            spotify_url TEXT,
            genres TEXT,
            followers_count INTEGER,
            images TEXT,
            current_date TEXT     
       )
       """
    
    cursor.execute(sql_query)

    try:
        logger.info("Inserting data in table........")
        df.to_sql("most_played_artists", engine, index=False, if_exists='append')
    except Exception as e:
            logger.error(f"Error inserting data: {e}")

    connection.close()
    logger.info("Database closed successfully")