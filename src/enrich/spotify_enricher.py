import os
import pandas as pd
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PROCESSED_DATA_FOLDER = os.path.join("data", "processed")

# --- AUTHENTICATION ---
def get_spotify_client():
    """Authenticates with Spotify using Client Credentials Flow"""
    return spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET")
    ))

sp = get_spotify_client()

def load_unified_data():
    """Load the merged dataset from Phase 4"""
    input_path = os.path.join(PROCESSED_DATA_FOLDER, "unified_music_history.parquet")
    if not os.path.exists(input_path):
        print("❌ No unified data available.")
        return None
    df = pd.read_parquet(input_path)
    print(f"✅ {len(df):,} total records loaded")
    return df

def get_unique_tracks(df):
    """Get unique tracks that need enrichment"""
    print("📊 Grouping tracks...")
    unique_tracks = df.groupby(['track', 'artist']).size().reset_index(name='play_count')
    unique_tracks = unique_tracks.sort_values(by='play_count', ascending=False)
    print(f"   Found {len(unique_tracks):,} unique songs.")
    
    print("\n🔥 Top 5 Most Played:")
    for _, row in unique_tracks.head(5).iterrows():
        print(f"  {row['play_count']:3}x - {row['track'].title()} by {row['artist'].title()}")
    
    return unique_tracks

def clean_spotify_id(spotify_id):
    """
    Clean Spotify ID to remove any prefixes
    Returns None if ID is invalid
    """
    if not spotify_id:
        return None
    
    sid = str(spotify_id).strip()
    
    # Filter out invalid IDs
    if any(x in sid.lower() for x in ['local', 'episode', 'http', 'unknown']):
        return None
    
    # Remove spotify:track: prefix if present
    if 'spotify:track:' in sid:
        sid = sid.split('spotify:track:')[1]
    
    # Remove any remaining colons or slashes
    sid = sid.replace(':', '').replace('/', '')
    
    # Spotify IDs are exactly 22 characters (Base62 encoding)
    if len(sid) == 22 and sid.isalnum():
        return sid
    
    return None

def clean_artist_for_search(artist_name):
    """
    Clean artist names before searching Spotify
    Removes YouTube artifacts like VEVO, Topic, etc.
    """
    if not artist_name or artist_name == 'unknown':
        return artist_name
    
    artist = str(artist_name).lower()
    
    # Remove common YouTube artifacts
    removals = ['atvevo', 'vevo', ' - topic', '- topic', ' topic', ' official', 'official']
    for item in removals:
        artist = artist.replace(item, '')
    
    # Clean spacing
    artist = ' '.join(artist.split()).strip()
    
    return artist if artist else 'unknown'

def search_track_on_spotify(track_name, artist_name):
    """Search for a track on Spotify"""
    try:
        # Clean artist name before searching
        clean_artist = clean_artist_for_search(artist_name)
        query = f"track:{track_name} artist:{clean_artist}"
        result = sp.search(q=query, type='track', limit=1)
        if result['tracks']['items']:
            return result['tracks']['items'][0]
    except Exception:
        pass
    return None

def build_id_lookup():
    """
    Build a lookup table of Spotify IDs from cleaned data
    Returns: dict mapping (track, artist) -> spotify_id
    """
    spotify_clean_path = os.path.join(PROCESSED_DATA_FOLDER, "spotify_cleaned.parquet")
    id_lookup = {}
    
    if not os.path.exists(spotify_clean_path):
        return id_lookup
    
    print("⚡ Building ID lookup from Spotify history...")
    df_spot = pd.read_parquet(spotify_clean_path)
    
    # Find URI column
    uri_col = next((col for col in df_spot.columns if 'uri' in col.lower()), None)
    
    if not uri_col:
        return id_lookup
    
    # Determine track/artist column names
    if 'track_name_clean' in df_spot.columns:
        track_col, artist_col = 'track_name_clean', 'artist_name_clean'
    elif 'track_name_cleaned' in df_spot.columns:
        track_col, artist_col = 'track_name_cleaned', 'artist_name_cleaned'
    elif 'track' in df_spot.columns:
        track_col, artist_col = 'track', 'artist'
    else:
        return id_lookup
    
    lookup_df = df_spot[[track_col, artist_col, uri_col]].dropna().drop_duplicates()
    
    for _, row in lookup_df.iterrows():
        clean_id = clean_spotify_id(row[uri_col])
        if clean_id:
            key = (row[track_col], row[artist_col])
            id_lookup[key] = clean_id
    
    print(f"   Cached {len(id_lookup):,} Spotify IDs!")
    return id_lookup

def extract_basic_metadata(track_data):
    """Extract basic track metadata (NOT audio features)"""
    if not track_data:
        return None
    
    try:
        metadata = {
            'spotify_id': track_data['id'],
            'spotify_artist_name': track_data['artists'][0]['name'].lower(),
            'album_name': track_data['album']['name'],
            'album_release_date': track_data['album']['release_date'],
            'album_art_url': track_data['album']['images'][0]['url'] if track_data['album']['images'] else None,
            'popularity': track_data['popularity'],
            'explicit': track_data['explicit'],
            'duration_ms_spotify': track_data['duration_ms'],
        }
        
        # Get genres from artist
        try:
            artist_id = track_data['artists'][0]['id']
            artist_data = sp.artist(artist_id)
            if artist_data and 'genres' in artist_data and artist_data['genres']:
                metadata['genres'] = ', '.join(artist_data['genres'])
            else:
                metadata['genres'] = 'Unknown'
        except:
            metadata['genres'] = 'Unknown'
        
        return metadata
    except Exception:
        return None

def get_audio_features_batch(spotify_ids):
    """
    Get audio features for multiple tracks at once (BATCH PROCESSING)
    Spotify allows up to 100 tracks per request
    """
    if not spotify_ids:
        return {}
    
    # Clean and validate all IDs
    clean_ids = [clean_spotify_id(sid) for sid in spotify_ids]
    clean_ids = [sid for sid in clean_ids if sid]
    
    if not clean_ids:
        return {}
    
    features_map = {}
    
    # Process in batches of 100 (Spotify's limit)
    for i in range(0, len(clean_ids), 100):
        batch = clean_ids[i:i+100]
        try:
            results = sp.audio_features(batch)
            
            if results:
                for idx, features in enumerate(results):
                    if features and isinstance(features, dict):
                        track_id = batch[idx]
                        features_map[track_id] = {
                            'energy': features.get('energy'),
                            'valence': features.get('valence'),
                            'danceability': features.get('danceability'),
                            'acousticness': features.get('acousticness'),
                            'instrumentalness': features.get('instrumentalness'),
                            'speechiness': features.get('speechiness'),
                            'tempo': features.get('tempo'),
                            'loudness': features.get('loudness'),
                        }
            
            # Small delay between batches
            if i + 100 < len(clean_ids):
                time.sleep(0.2)
        
        except Exception:
            # If batch fails, continue with next batch
            continue
    
    return features_map

def enrich_tracks_optimized(unique_tracks, sample_size=None):
    """
    OPTIMIZED: Enrichment with batch processing
    1. Build ID lookup
    2. Search for missing tracks
    3. Batch fetch ALL audio features at once
    """
    print("\n🔬 Starting OPTIMIZED enrichment...")
    
    # Sample mode
    if sample_size:
        print(f"⚠️  SAMPLE MODE: Only enriching {sample_size} tracks")
        unique_tracks = unique_tracks.head(sample_size)
    
    total = len(unique_tracks)
    
    # Step 1: Build ID lookup
    id_lookup = build_id_lookup()
    
    # Step 2: Find or search for all tracks
    print(f"\n🔍 Phase 1: Finding tracks on Spotify...")
    enriched_data = []
    tracks_needing_search = []
    
    for idx, row in unique_tracks.iterrows():
        track_name = row['track']
        artist_name = row['artist']
        
        metadata = None
        lookup_key = (track_name, artist_name)
        
        # Try lookup first
        if lookup_key in id_lookup:
            spotify_id = id_lookup[lookup_key]
            try:
                track_data = sp.track(spotify_id)
                metadata = extract_basic_metadata(track_data)
            except:
                pass
        
        # If not found, search
        if not metadata:
            track_data = search_track_on_spotify(track_name, artist_name)
            if track_data:
                metadata = extract_basic_metadata(track_data)
        
        if metadata:
            enriched_data.append({
                'track': track_name,
                'artist': artist_name,
                **metadata
            })
        else:
            # Track not found
            enriched_data.append({
                'track': track_name,
                'artist': artist_name,
                'spotify_id': None,
                'genres': 'Unknown'
            })
        
        # Progress
        if (len(enriched_data)) % 50 == 0:
            print(f"   {len(enriched_data)}/{total} tracks processed...")
    
    print(f"✅ Found {sum(1 for d in enriched_data if d.get('spotify_id'))} tracks on Spotify")
    
    # Step 3: Batch fetch ALL audio features at once
    print(f"\n🎵 Phase 2: Fetching audio features (BATCH MODE)...")
    
    spotify_ids = [d['spotify_id'] for d in enriched_data if d.get('spotify_id')]
    
    if spotify_ids:
        print(f"   Fetching features for {len(spotify_ids)} tracks...")
        features_map = get_audio_features_batch(spotify_ids)
        print(f"   Retrieved {len(features_map)} audio feature sets")
        
        # Step 4: Merge audio features back into enriched data
        for item in enriched_data:
            if item.get('spotify_id') and item['spotify_id'] in features_map:
                item.update(features_map[item['spotify_id']])
    
    return pd.DataFrame(enriched_data)

def merge_enriched_data(original_df, enriched_df):
    """Merge enriched metadata back into listening history"""
    print("\n🤝 Merging enriched data...")
    final_df = original_df.merge(enriched_df, on=['track', 'artist'], how='left')
    print(f"✅ Merged! Final dataset has {len(final_df):,} rows and {len(final_df.columns)} columns")
    return final_df

def show_enrichment_summary(df):
    """Show enrichment statistics"""
    print("\n" + "="*70)
    print("📊 ENRICHMENT SUMMARY")
    print("="*70)
    
    # Success rate
    total_tracks = df[['track', 'artist']].drop_duplicates().shape[0]
    found_tracks = df[df['spotify_id'].notna()][['track', 'artist']].drop_duplicates().shape[0]
    success_rate = (found_tracks / total_tracks) * 100 if total_tracks > 0 else 0
    print(f"\n✅ Found on Spotify: {found_tracks:,}/{total_tracks:,} ({success_rate:.1f}%)")
    
    # Audio features check
    if 'energy' in df.columns:
        tracks_with_features = df[df['energy'].notna()][['track', 'artist']].drop_duplicates().shape[0]
        feature_rate = (tracks_with_features / found_tracks) * 100 if found_tracks > 0 else 0
        print(f"🎵 Audio features: {tracks_with_features:,}/{found_tracks:,} ({feature_rate:.1f}%)")
    
    # Top genres
    if 'genres' in df.columns:
        print("\n🎸 Top 5 Genres:")
        all_genres = []
        for genres_str in df['genres'].dropna():
            if genres_str != 'Unknown':
                all_genres.extend([g.strip() for g in str(genres_str).split(',')])
        if all_genres:
            for genre, count in pd.Series(all_genres).value_counts().head(5).items():
                print(f"  {genre}: {count:,} listens")
    
    # Audio features averages
    if 'energy' in df.columns:
        print("\n🎵 Your Listening Vibe:")
        print(f"  Energy:       {df['energy'].mean():.2f} (0=calm, 1=energetic)")
        print(f"  Happiness:    {df['valence'].mean():.2f} (0=sad, 1=happy)")
        print(f"  Danceability: {df['danceability'].mean():.2f}")
        print(f"  Tempo:        {df['tempo'].mean():.0f} BPM")
    
    # Corrected artist names
    if 'spotify_artist_name' in df.columns:
        mismatches = df[
            (df['artist'] != df['spotify_artist_name']) & 
            (df['spotify_artist_name'].notna())
        ][['artist', 'spotify_artist_name']].drop_duplicates().head(5)
        
        if len(mismatches) > 0:
            print("\n🔧 Artist Name Corrections (Sample):")
            for _, row in mismatches.iterrows():
                print(f"  '{row['artist']}' → '{row['spotify_artist_name']}'")
    
    print("\n" + "="*70)

def save_enriched_data(df):
    """Save the final enriched dataset"""
    print("\n💾 Saving enriched data...")
    output_path = os.path.join(PROCESSED_DATA_FOLDER, "enriched_music_history.parquet")
    df.to_parquet(output_path, index=False)
    print(f"✅ Saved to: {output_path}")
    print(f"   Total listens: {len(df):,}")
    print(f"   Total columns: {len(df.columns)}")
    return output_path

def run_enrichment(sample_mode=False, sample_size=50):
    """Main enrichment orchestrator"""
    print("🔬 PHASE 5: OPTIMIZED ENRICHMENT")
    print("Fetching genres, moods, and audio features from Spotify!\n")
    
    start_time = time.time()
    
    # Load data
    df = load_unified_data()
    if df is None:
        return
    
    # Get unique tracks
    unique_tracks = get_unique_tracks(df)
    
    # Enrich (OPTIMIZED!)
    enriched_df = enrich_tracks_optimized(
        unique_tracks,
        sample_size=sample_size if sample_mode else None
    )
    
    # Merge back
    final_df = merge_enriched_data(df, enriched_df)
    
    # Show summary
    show_enrichment_summary(final_df)
    
    # Save
    save_enriched_data(final_df)
    
    elapsed = time.time() - start_time
    print(f"\n⏱️  Total time: {elapsed/60:.1f} minutes")
    print("\n✨ ENRICHMENT COMPLETE! ✨")

if __name__ == "__main__":
    print("="*70)
    print("SAMPLE MODE: Test with 50 tracks first!")
    print("="*70 + "\n")
    
    # Start with sample mode to test
    SAMPLE_MODE = False
    run_enrichment(sample_mode=SAMPLE_MODE, sample_size=50)