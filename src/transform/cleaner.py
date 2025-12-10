import os
import pandas as pd

PROCESSED_DATA_FOLDER = os.path.join("data", "processed")

def clean_track_name(track_name):
    if pd.isna(track_name) or track_name == "":
        return "unknown"