import os
import pandas as pd

PROCESSED_DATA_FOLDER = os.path.join("data", "processed")

def clean_track_name(track_name):
    if pd.isna(track_name) or track_name == "":
        return "unknown"
    
    # converting track name to string and lower case
    track_name = str(track_name).lower()


    # creating a list of things to remove from track name
    things_to_remove = [
        '(feat.', '(ft.', '(featuring', '(with', '(official video)',
        '(official audio)', '(official music video)', '(lyric video)', '(lyrics)',
        '(lyric)', '(audio)', '(video)', '(visualizer)', '(visualiser)',
        '(original version)', '(radio version)', '(radio edit)', '(album version)',
        '(remix)', '(extended version)', '(extended mix)', '(remastered)', '['
        '[official video]', '[official audio]', '[lyrics]', '[lyric video]',
    ]

    # looping through the list to remove any of the markers if theyy exist
    for marker in things_to_remove:
        if marker in track_name:
            track_name = track_name.split(marker)[0] # picking the track name only in the list created

    # cleaning up the result
    track_name = track_name.strip()
    track_name = " ".join(track_name.split())
    track_name = track_name.strip(" ._-")

    return track_name if track_name else "unknown"

def clean_artist_name(artist_name):

    # return unkown if artist name is empty
    if pd.isna(artist_name) or artist_name == "":
        return "unknown"
    
    # convert to lowercase
    artist_name = str(artist_name).lower()

    # handling the ' - topic' addition to artist name in yt data
    if " - topic" in artist_name:
        artist_name = artist_name.split(" - topic")[0]
    
    if "- topic" in artist_name:
        artist_name = artist_name.split("- topic")[0]

    # handling multiple artists
    if "," in artist_name:
        artist_name = artist_name.split(",")[0]

    return artist_name if artist_name else "unknown"

def clean_spotify_data():
    """
    Load, clean and save spotify data
    """
    # get the path for the file to be cleaned
    input_path = os.path.join(PROCESSED_DATA_FOLDER, "Spotify_history.parquet")

    # check if the file exists
    if not os.path.exists(input_path):
        print("No spotify file found")
        return None
    
    #load the file into a dataframe
    df = pd.read_parquet(input_path)
    print(f"loaded {len(df)} spotify records")

    #clean the track name and artist name columns
    df['track_name_cleaned'] = df['track_name'].apply(clean_track_name)
    df['artist_name_cleaned'] = df['artist_name'].apply(clean_artist_name)

    #check the cleaning that was done
    if len(df) > 0:
        sample = df[['track_name','track_name_cleaned', 'artist_name', 'artist_name_cleaned']].sample(5)
        for idx, rows in sample.iterrows():
            print(f"Original: {rows['track_name']} | {rows['artist_name']}")
            print(f"Cleaned: {rows['track_name_cleaned']} | {rows['artist_name_cleaned']}")
            print()

    save_path = os.path.join(PROCESSED_DATA_FOLDER, "spotify_cleaned")
    df.to_parquet(save_path, index=False)
    print("cleaned spotify data saved")

    return df

def clean_youtube_data():
    """
    Load, clean and save youtube data
    """
    # get the path for the file to be cleaned
    input_path = os.path.join(PROCESSED_DATA_FOLDER, "youtube_history_2025.parquet")

    # check if the file exists
    if not os.path.exists(input_path):
        print("No youtube file found")
        return None
    
    #load the file into a dataframe
    df = pd.read_parquet(input_path)
    print(f"loaded {len(df)} youtube records")

    #clean the track name and artist name columns
    df['track_name_cleaned'] = df['track_name'].apply(clean_track_name)
    df['artist_name_cleaned'] = df['artist_name'].apply(clean_artist_name)

    #check the cleaning that was done
    if len(df) > 0:
        sample = df[['track_name','track_name_cleaned', 'artist_name', 'artist_name_cleaned']].head(5)
        for idx, rows in sample.iterrows():
            print(f"Original: {rows['track_name']} | {rows['artist_name']}")
            print(f"Cleaned: {rows['track_name_cleaned']} | {rows['artist_name_cleaned']}")
            print()

    save_path = os.path.join(PROCESSED_DATA_FOLDER, "youtube_cleaned")
    df.to_parquet(save_path, index=False)
    print("cleaned youtube data saved")

    return df


def run_quality_check():
    spotify_path = os.path.join(PROCESSED_DATA_FOLDER, "spotify_cleaned.parquet")
    if os.path.exists(spotify_path):
        df = pd.read_parquet(spotify_path)
        original_unique = df['track_name'].nunique()
        cleaned_unique = df['track_name_cleaned'].nunique()
        print(f"\n SPOTIFY:")
        print(f"  Unique tracks (raw):     {original_unique:,}")
        print(f"  Unique tracks (cleaned): {cleaned_unique:,}")
        print(f"  Duplicates removed:      {original_unique - cleaned_unique:,}")

    youtube_path = os.path.join(PROCESSED_DATA_FOLDER, "youtube_cleaned.parquet")
    if os.path.exists(youtube_path):
        df = pd.read_parquet(youtube_path)
        original_unique = df['track_name'].nunique()
        cleaned_unique = df['track_name_cleaned'].nunique()
        print(f"\n YOUTUBE:")
        print(f"  Unique tracks (raw):     {original_unique:,}")
        print(f"  Unique tracks (cleaned): {cleaned_unique:,}")
        print(f"  Duplicates removed:      {original_unique - cleaned_unique:,}")

if __name__ == "__main__":
    spotify_df = clean_spotify_data()
    youtube_df = clean_youtube_data()

    
    run_quality_check()