import pandas as pd
import aiohttp
import asyncio
from requests.auth import HTTPBasicAuth
import requests

# Spotify API credentials
client_id = '646f6ce95d424bfe9b930483c7e4f7ec'
client_secret = '7d9d369e0fd54dc688029a485feb7147'

# Function to get Spotify API token
def get_spotify_token(client_id, client_secret):
    url = 'https://accounts.spotify.com/api/token'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'client_credentials'
    }
    response = requests.post(url, headers=headers, data=data, auth=HTTPBasicAuth(client_id, client_secret))
    token_info = response.json()
    return token_info['access_token']

# Asynchronous function to get cover URL from Spotify API
async def get_cover_url(session, token, track_name, artist_name):
    search_url = 'https://api.spotify.com/v1/search'
    headers = {
        'Authorization': f'Bearer {token}'
    }
    params = {
        'q': f'track:{track_name} artist:{artist_name}',
        'type': 'track',
        'limit': 1
    }
    async with session.get(search_url, headers=headers, params=params) as response:
        if response.status == 200:
            results = await response.json()
            if results['tracks']['items']:
                return results['tracks']['items'][0]['album']['images'][0]['url']
    return None

# Function to fetch cover URLs for all tracks
async def fetch_all_cover_urls(data, token):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for index, row in data.iterrows():
            task = asyncio.ensure_future(get_cover_url(session, token, row['track_name'], row['artist(s)_name']))
            tasks.append(task)
        cover_urls = await asyncio.gather(*tasks)
        return cover_urls

# Main function to run the script
def main():
    # Get the token
    spotify_token = get_spotify_token(client_id, client_secret)

    # Load the dataset
    file_path = 'spotify-2023.csv'  # Replace with the actual path to your CSV file
    spotify_data = pd.read_csv(file_path, encoding='latin1')

    # Fetch cover URLs
    loop = asyncio.get_event_loop()
    cover_urls = loop.run_until_complete(fetch_all_cover_urls(spotify_data, spotify_token))

    # Add the cover URLs to the dataframe
    spotify_data['cover_url'] = cover_urls

    # Save the updated dataframe to a new CSV file
    updated_file_path = 'spotify-2023-with-cover-urls.csv'  # Replace with the actual path to save the new CSV file
    spotify_data.to_csv(updated_file_path, index=False)

if __name__ == '__main__':
    main()
