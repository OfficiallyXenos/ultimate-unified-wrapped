import os
import pandas as pd
import sys
import time

# Add SRC/ to path so we can import spotify_autj
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spotify_auth import get_spotify_client

# initialize the spotify client
sp = get_spotify_client()

PROCESSED_DATA_FOLDER = os.path.join("data", "processed")

def load_unified_data():
    """
    Load the merged dataset
    """
    input_path = os.path.join(PROCESSED_DATA_FOLDER, "unified_music_history.parquet")

    # checking if the file is available
    if not os.path.exists(input_path):
        print("No unified data available for loading")
        return None
    
    df = pd.read_parquet(input_path)
    print(f"{len(df):,} total records loaded")

    return df

def get_unique_tracks(df):
    """
    Get the unique tracks needed for enrichment
    """
    unique_tracks = df.groupby(['tracks', 'artist']).size().reset_index(name='play_count')
    unique_tracks = df.sort_values(by='play_count')

    return unique_tracks

def search_track_on_spotify(track_name, artist_name):
    """
    search for tracks on spotify and returns their metadata
    Returns None if it finds nothing
    """

    try:
        # searching for the track
        query = f"track:{track_name} artist:{artist_name}"
        result = sp.search(q=query, type='track', limit=1)

        if result['tracks']['items']:
            track = result['tracks']['items'][0]
            return track 
        else:
            return None
    except Exception as e:
        print(f"Error trying to search for {track_name}: {str(e)}")
        return None
