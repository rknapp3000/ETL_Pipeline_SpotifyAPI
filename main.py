import sqlalchemy
from pandas import DataFrame
import sqlalchemy
import requests
import sqlite3
import os
from urllib.parse import urlencode
import base64
from flask import Flask, redirect, request
from typing import Dict
import logging
from datetime import datetime



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

DATABASE= "sqlite:///most_played_artists.sqlite"
USER_ID = os.getenv('USER_ID')
TOKEN = os.getenv('TOKEN')

CLIEND_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
redirect_uri = 'http://127.0.0.1:8888/callback'

#Flask
app = Flask(__name__)


#Request User Authorization
@app.route('/')
def login():

    scope = 'user-top-read'

    # Query parameters for Spotify's /authorize endpoint
    query_params = {
        'response_type': 'code',
        'client_id': CLIEND_ID,
        'scope': scope,
        'redirect_uri': redirect_uri,
        # 'state': state
    }

    # Build the authorization URL and redirect the user
    authorization_url = 'https://accounts.spotify.com/authorize?' + urlencode(query_params)
    return redirect(authorization_url)


# Handle the Spotify callback and request access token
@app.route('/callback')
def callback():
    # Get the query parameters from the URL
    code = request.args.get('code')
    error = request.args.get('error')

    # If the error is present in the URL, that means the user denied access
    assert not error, f"Authorization failed: {error}"  # If error is present, raise AssertionError
    assert code, "Authorization code not found. Authorization failed."  # If no code, raise AssertionError

    #Exchange the authorization code for an access token
    token_url = 'https://accounts.spotify.com/api/token'
    auth_string = f"{CLIEND_ID}:{CLIENT_SECRET}"
    auth_base64 = base64.b64encode(auth_string.encode()).decode()

    headers = {
        'Authorization': f'Basic {auth_base64}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    data = {
        'code': code,
        'redirect_uri': redirect_uri,
        'grant_type': 'authorization_code'
    }

    # Make a POST request to the /api/token endpoint
    response = requests.post(token_url, headers=headers, data=data)

    # If the request was successful, we'll get the access token
    if response.status_code == 200:
        token_info = response.json()
        access_token = token_info.get('access_token')
        refresh_token = token_info.get('refresh_token')
        expires_in = token_info.get('expires_in')
        
        user_data = get_user_data(access_token)
        transformed_data = transform_data(user_data)
        load_data(transformed_data)
        
        # return transformed_data
        return user_data
    else:
        return f"Error exchanging code for token: {response.status_code} - {response.text}"
    
    
def get_user_data(access_token):
    # Spotify API endpoint for getting user data
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

 
def load_data(df: DataFrame): 
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
    
    
    



if __name__ == "__main__":
    
    # Run the Flask application on port 8888
    app.run(debug=True, port=8888)



