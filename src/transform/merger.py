import os
import pandas as pd

PROCESSED_DATA_FOLDER = os.path.join("data", "processed")

def load_clean_data():
    """
    load both spotify and youtube data
    """
    spotify_path = os.path.join(PROCESSED_DATA_FOLDER, "spotify_cleaned.parquet")
    youtube_path = os.path.join(PROCESSED_DATA_FOLDER, "youtube_cleaned.parquet")

    spotify_df = None
    youtube_df = None

    if os.path.exists(spotify_path):
        spotify_df = pd.read_parquet(spotify_path)
        print(f"loaded {len(spotify_df):,} Spotify records")
    else:
        print("No Spotify data found")

    if os.path.exists(youtube_path):
        youtube_df = pd.read_parquet(PROCESSED_DATA_FOLDER, "youtube_cleaned.parquet")
        print(f"loaded {len(youtube_df):,} YouTube records")
    else:
        print("No Youtube Music found")

    return spotify_df, youtube_df

def prepare_for_merge(spotify_df, youtube_df):
    """
    This function takes both dataframes and prepares them for mergeing
    """
    if spotify_df is not None:
        spotify_prepared = spotify_df[[
            'timestamp',
            'track_name_cleaned',
            'artist_name_cleaned',
            'duration',
            'skipped'
        ]].copy()

        spotify_prepared['source'] = 'spotify'

        spotify_prepared = spotify_prepared.rename(columns = {
            'track_name_cleaned': 'track',
            'artist_name_cleaned': 'artist'
        })

        print(f"  Spotify: {len(spotify_prepared):,} records prepared")
    else:
        spotify_prepared = None
    
    if youtube_df is not None:
        youtube_prepared = youtube_df[[
            'track_name_cleaned',
            'artist_name_cleaned',
            'timestamp'
        ]].copy()

        youtube_prepared['source'] = "youtube music"

        youtube_prepared = youtube_prepared.rename(columns = {
            'track_name_cleaned': 'track',
            'artist_name_cleaned': 'artist'
        })

        youtube_prepared['duration_ms'] = None
        youtube_prepared['skipped'] = None

        print(f"  YouTube: {len(youtube_prepared):,} records prepared")
    else:
        youtube_prepared = None


