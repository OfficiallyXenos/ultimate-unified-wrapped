import os
import json
import pandas as pd

RAW_DATA_FOLDER = os.path.join("data", "raw")
PROCESSED_DATA_FOLDER = os.path.join("data", "processed")

def load_spotify_data():
    print('looking for spotify files')

    all_files_in_folder = os.listdir(RAW_DATA_FOLDER)
    master_songs_list = []

    for filename in all_files_in_folder:
        if filename.startswith("Streaming_History_Audio_") and filename.endswith(".json"):
            full_path = os.path.join(RAW_DATA_FOLDER, filename)
            print(f"Reading {filename}")

            try:
                with open(full_path, 'r', encoding= 'utf-8') as file:
                    data = json.load(file)
                    master_songs_list.extend(data)
            
            except Exception as e:
                print(f"error reading {filename}: {e}")

    if not master_songs_list:
        print("No Files Found")
        return
    
    df = pd.DataFrame(master_songs_list)

        # Rename columns
    df = df.rename(columns={
        'master_metadata_track_name': 'track_name',
        'master_metadata_album_artist_name': 'artist_name',
        'master_metadata_album_album_name': 'album_name',
        'spotify_track_uri': 'spotify_uri',
        'ts': 'timestamp',
        'ms_played': 'duration_ms',
    })

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    df_2025 = df[df['timestamp'].dt.year == 2025].copy()

        # 3. Check if we have data left
    if df_2025.empty:
        print("After filtering, there are no songs left for 2025.")
        print("(Check if your JSON files actually cover the year 2025!)")
        return
    else:
        print(f"Filter Success! We threw away old songs.")
        print(f"Songs remaining for 2025: {len(df_2025)}")

    os.makedirs(PROCESSED_DATA_FOLDER, exist_ok= True)
    save_path = os.path.join(PROCESSED_DATA_FOLDER, "Spotify_history_2025.parquet")

    df_2025.to_parquet(save_path)
    print(f"Saved 2025 data to: {save_path}")

if __name__ == "__main__":
    load_spotify_data()
