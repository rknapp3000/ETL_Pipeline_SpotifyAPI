import requests
import os
from urllib.parse import urlencode
import base64
from flask import Flask, redirect, request
import logging
from util import get_user_data, transform_data, load_data



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

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
    


if __name__ == "__main__":
    
    # Run the Flask application on port 8888
    app.run(debug=True, port=8888)
