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
        youtube_df = pd.read_parquet(youtube_path)
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
            'duration_ms',
            'skipped'
        ]].copy()

        spotify_prepared['source'] = 'spotify'

        spotify_prepared = spotify_prepared.rename(columns = {
            'track_name_cleaned': 'track',
            'artist_name_cleaned': 'artist'
        })

        print(f"Spotify: {len(spotify_prepared):,} records prepared")
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

        print(f"YouTube: {len(youtube_prepared):,} records prepared")
    else:
        youtube_prepared = None

    return spotify_prepared, youtube_prepared

def merge_datasets(spotifty_prepared, youtube_prepared):
    dfs_to_merge = []

    if spotifty_prepared is not None:
        dfs_to_merge.append(spotifty_prepared)
    
    if youtube_prepared is not None:
        dfs_to_merge.append(youtube_prepared)

    if not dfs_to_merge:
        print("No datasets to merge")
        return None
    
    merged_df = pd.concat(dfs_to_merge, ignore_index=True)

    merged_df = merged_df.sort_values("timestamp").reset_index(drop=True)

    print(f"Merge Successful! Total records are now {len(merged_df):,}")

    return merged_df

def add_columns(merged_df):
    """
    Add some extra columns that will be useful for the Wrapped analysis:
    """
    merged_df['date'] = merged_df['timestamp'].dt.date
    merged_df['month'] = merged_df['timestamp'].dt.month
    merged_df['month_name'] = merged_df['timestamp'].dt.month_name()
    merged_df['day_of_week'] = merged_df['timestamp'].dt.day_name()
    merged_df['hour'] = merged_df['timestamp'].dt.hour
    merged_df['week_of_year'] = merged_df['timestamp'].dt.isocalendar().week

    print("  Added: date, hour, day_of_week, month, month_name, week_of_year")
    
    return merged_df

def show_merge_summary(df):
    """
    Show a nice summary of the merged data
    """
    print("\n" + "="*70)
    print("MERGE SUMMARY")
    print("="*70)
    
    
    print("\n Records by Source:")
    source_counts = df['source'].value_counts()
    for source, count in source_counts.items():
        percentage = (count / len(df)) * 100
        print(f"  {source.capitalize()}: {count:,} ({percentage:.1f}%)")
    
    
    print(f"\n Date Range:")
    print(f"  First listen: {df['timestamp'].min()}")
    print(f"  Last listen:  {df['timestamp'].max()}")
    
    
    print(f"\n Unique Content:")
    print(f"  Unique tracks:  {df['track'].nunique():,}")
    print(f"  Unique artists: {df['artist'].nunique():,}")
    
    
    print(f"\n Top 5 Most Played Tracks:")
    top_tracks = df.groupby(['track', 'artist']).size().reset_index(name='plays')
    top_tracks = top_tracks.sort_values('plays', ascending=False).head(5)
    
    for idx, row in top_tracks.iterrows():
        print(f"  {row['plays']:3} plays - {row['track'].title()} by {row['artist'].title()}")
    
    
    spotify_only = df[df['source'] == 'spotify'].copy()
    if len(spotify_only) > 0 and 'skipped' in spotify_only.columns:
        total_spotify = len(spotify_only)
        skipped_count = spotify_only['skipped'].sum()
        skip_rate = (skipped_count / total_spotify) * 100
        print(f"\n Skip Statistics (Spotify only):")
        print(f"  Total Spotify listens: {total_spotify:,}")
        print(f"  Skipped: {int(skipped_count):,} ({skip_rate:.1f}%)")
        print(f"  Completed: {total_spotify - int(skipped_count):,} ({100-skip_rate:.1f}%)")
    
    
    print(f"\n Listens by Month:")
    monthly = df.groupby('month_name').size()
    
    month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                   'July', 'August', 'September', 'October', 'November', 'December']
    for month in month_order:
        if month in monthly.index:
            print(f"  {month}: {monthly[month]:,} listens")
    
    print("\n" + "="*70)

def save_merged_data(df):
    save_path = os.path.join(PROCESSED_DATA_FOLDER, "unified_music_history.parquet")
    df.to_parquet(save_path, index=False)

    print(f"Unified music history saved to {save_path}")

    return save_path

def run_merger():
    spotify_df, youtube_df = load_clean_data()

    spotify_prepared, youtube_prepared = prepare_for_merge(spotify_df, youtube_df)

    merged_df = merge_datasets(spotify_prepared, youtube_prepared)

    merged_df = add_columns(merged_df)

    show_merge_summary(merged_df)

    save_path = save_merged_data(merged_df)

    print("Merge Complete")

if __name__ == "__main__":
    run_merger()