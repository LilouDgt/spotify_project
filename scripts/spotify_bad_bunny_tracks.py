# spotify_albums_pipeline_fast_with_progress.py

import os
import base64
import requests
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import datetime

# -----------------------------
# Step 0: Load environment vars
# -----------------------------
load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
BQ_TABLE = "spotify-data-genre.spotify_data.raw_bad_bunny_track"

# -----------------------------
# Step 1: Authenticate with Spotify
# -----------------------------
auth_str = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
b64_auth_str = base64.b64encode(auth_str.encode()).decode()

auth_url = "https://accounts.spotify.com/api/token"
auth_headers = {"Authorization": f"Basic {b64_auth_str}"}
auth_data = {"grant_type": "client_credentials"}

auth_response = requests.post(auth_url, headers=auth_headers, data=auth_data)
token = auth_response.json()["access_token"]
print("Spotify token obtained!")

headers = {"Authorization": f"Bearer {token}"}

# -----------------------------
# Step 2: Get Artist ID for Bad Bunny
# -----------------------------
artist_name = "Bad Bunny"
search_url = f"https://api.spotify.com/v1/search?q={artist_name}&type=artist&limit=1"
response = requests.get(search_url, headers=headers)
artist_id = response.json()["artists"]["items"][0]["id"]
print(f"Bad Bunny Artist ID: {artist_id}")

# -----------------------------
# Step 3: Get all albums (filtered & deduplicated)
# -----------------------------
albums = []
seen_albums = set()
url = f"https://api.spotify.com/v1/artists/{artist_id}/albums?limit=50"

while url:
    resp = requests.get(url, headers=headers).json()
    for album in resp['items']:
        if album['album_type'] not in ['album', 'single']:
            continue
        normalized_name = album['name'].lower().split('(')[0].strip()
        if normalized_name not in seen_albums:
            albums.append(album)
            seen_albums.add(normalized_name)
    url = resp.get('next')

print(f"Total unique albums/singles found: {len(albums)}")

# -----------------------------
# Helper: normalize release date
# -----------------------------
def normalize_release_date(date_str, precision):
    if precision == 'year':
        return f"{date_str}-01-01"
    elif precision == 'month':
        return f"{date_str}-01"
    return date_str  # already YYYY-MM-DD

# -----------------------------
# Step 4: Get all tracks with popularity (batch + progress)
# -----------------------------
tracks_data = []
seen_tracks = set()
total_albums = len(albums)

for album_index, album in enumerate(albums, start=1):
    album_id = album['id']
    album_name = album['name']
    album_release = normalize_release_date(album['release_date'], album['release_date_precision'])
    
    print(f"\nProcessing album {album_index}/{total_albums}: {album_name}")
    
    tracks_url = f"https://api.spotify.com/v1/albums/{album_id}/tracks?limit=50"
    track_ids_batch = []
    track_count_in_album = 0

    while tracks_url:
        track_resp = requests.get(tracks_url, headers=headers).json()
        if 'items' not in track_resp:
            break
        for track in track_resp['items']:
            track_id = track['id']
            if track_id in seen_tracks:
                continue
            seen_tracks.add(track_id)
            track_ids_batch.append(track_id)
            track_count_in_album += 1
            
            # Process batch of 50 tracks
            if len(track_ids_batch) == 50:
                batch_url = f"https://api.spotify.com/v1/tracks?ids={','.join(track_ids_batch)}"
                batch_resp = requests.get(batch_url, headers=headers).json()
                for t in batch_resp['tracks']:
                    tracks_data.append({
                        "id": t['id'],
                        "name": t['name'],
                        "popularity": t['popularity'],
                        "release_date": album_release,
                        "album_name": album_name
                    })
                track_ids_batch = []
        tracks_url = track_resp.get('next')
    
    # Process remaining tracks
    if track_ids_batch:
        batch_url = f"https://api.spotify.com/v1/tracks?ids={','.join(track_ids_batch)}"
        batch_resp = requests.get(batch_url, headers=headers).json()
        for t in batch_resp['tracks']:
            tracks_data.append({
                "id": t['id'],
                "name": t['name'],
                "popularity": t['popularity'],
                "release_date": album_release,
                "album_name": album_name
            })

    print(f"  Total tracks processed for this album: {track_count_in_album}")
    print(f"  Total unique tracks collected so far: {len(tracks_data)}")

print(f"\nAll albums processed. Total unique tracks extracted: {len(tracks_data)}")

# -----------------------------
# Step 5: Load into BigQuery
# -----------------------------
client = bigquery.Client()

schema = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("popularity", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("release_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("album_name", "STRING", mode="NULLABLE"),
]

# Create table if not exists
try:
    table = bigquery.Table(BQ_TABLE, schema=schema)
    client.create_table(table)
    print(f"Table {BQ_TABLE} created.")
except Exception as e:
    print("Table already exists, skipping creation.")

# Insert in batches of 500
batch_size = 500
total_records = len(tracks_data)
for i in range(0, total_records, batch_size):
    batch = tracks_data[i:i+batch_size]
    errors = client.insert_rows_json(BQ_TABLE, batch)
    if errors:
        print(f"Errors in batch {i//batch_size + 1}: {errors}")
    else:
        print(f"Inserted batch {i//batch_size + 1} ({len(batch)} records)")

print(f"All {total_records} tracks successfully loaded into BigQuery.")
