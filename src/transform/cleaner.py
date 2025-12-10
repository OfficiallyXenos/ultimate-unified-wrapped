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
    track_name = track_name.split(" ._-")

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


