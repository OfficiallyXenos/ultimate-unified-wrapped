import pandas as pd
import os

# Load your raw saved data
# (Assuming you successfully ran the loaders from Phase 2)
yt_path = "data/processed/youtube_history_2025.parquet"
sp_path = "data/processed/spotify_history.parquet"

print("--- YOUTUBE SAMPLE (The Messy One) ---")
if os.path.exists(yt_path):
    df_yt = pd.read_parquet(yt_path)
    # Sample 20 random rows to see the variety of mess
    df_yt.sample(100).to_csv("yt_sample.csv", index=False)
    print("YT sample saved")
else:
    print("YouTube data not found.")

print("\n--- SPOTIFY SAMPLE (The Cleaner One) ---")
if os.path.exists(sp_path):
    df_sp = pd.read_parquet(sp_path)
    df_sp.sample(100).to_csv("spot_sample.csv", index=False)
    print("spotify sample saved")
else:
    print("Spotify data not found.")