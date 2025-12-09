import os
import json
import pandas as pd

PROCESSED_DATA_FOLDER = os.path.join("data", "processed")

def load_youtube_data():

    print('looking for youtube watch history file')
    filepath = os.path.join("data",  "raw", "watch-history.json")

    #checking if watch-history exists
    if not os.path.exists(filepath):
        print("No Files Found")
        return
    
    #opening the file and loading it into a dataframe
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            data = json.load(file)
    except Exception as e:
        print(f"error reading watch-history.json: {e}")
        return
    
    print(f"{len(data)} rows found")

    # loading into a dataframe
    df = pd.DataFrame(data)

    # since watch-history.json contain all youtube data, we filter to get only music data
    if 'header' in df.columns:
        music_df = df[df['header'] == "YouTube Music"].copy()
        print(f" found only {len(music_df)} rows of music data")
    else: 
        music_df = df.copy()

    # cleaning the title column by removing "watched " to get only the track name
    music_df['track_name'] = music_df['title'].str.replace("Watched ", "", regex=False)

    # converting the time column to the appropriate dtype
    music_df['timestamp'] = pd.to_datetime(music_df['time'], format="ISO8601")

    # extracting the artist name from the subtitle colummn using a function
    def get_artist_name(subtitle_cell):
        if isinstance(subtitle_cell, list) and len(subtitle_cell) > 0:
            return subtitle_cell[0].get('name')
        return "unknown"
    
    music_df['artist_name'] = music_df['subtitles'].apply(get_artist_name)

    #filtering for music listended to in only 2025
    df_2025 = music_df[music_df['timestamp'].dt.year == 2025].copy()

    # Select only the columns we need
    final_df = df_2025[['track_name', 'artist_name', 'timestamp', 'titleUrl']]

    #saving final file
    save_path = os.path.join(PROCESSED_DATA_FOLDER, "youtube_history_2025.parquet")
    final_df.to_parquet(save_path)

    print(f"saved to {save_path}, found {len(final_df)} songs")
    
if __name__ == "__main__":
    load_youtube_data()
    

