import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv

load_dotenv()

def get_spotify_client():
    """
    Returns an authenticated Spotify client
    """
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=os.getenv("SPOTIFY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
    redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
    scope="user-read-recently-played"
    ))

    return sp

if __name__ == "__main__":
    sp = get_spotify_client()
    me = sp.me()
    print(me["display_name"])
