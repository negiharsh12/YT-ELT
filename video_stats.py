import requests
import json

import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_HANDLE = "tseries"
MAX_RESULTS_PER_PAGE = 50

def get_channel_playlist_id():
    
    try:
        
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        response.raise_for_status()  # Check if the request was successful

        data = response.json()

        # print(json.dumps(data, indent=4))

        channel_items = data["items"][0]
        channel_playlist_id = channel_items["contentDetails"]["relatedPlaylists"]['uploads']

        # print(channel_playlist_id)

        return channel_playlist_id
    
    except requests.exceptions.RequestException as e:
        raise e
    
def get_video_ids(playlist_id):
    try:
        video_ids = []
        next_page_token = ""

        while True:
            url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={MAX_RESULTS_PER_PAGE}&pageToken={next_page_token}&playlistId={playlist_id}&key={API_KEY}"
            
            response = requests.get(url)
            response.raise_for_status()  # Check if the request was successful
            data = response.json()

            for item in data.get("items", []):
                video_ids.append(item["contentDetails"]["videoId"])
            # for i in range(current_page_count):
            #     video_ids.append(data["items"][i]["contentDetails"]['videoId'])
            print(f"Fetched {len(video_ids)} videos so far...")

            if 'nextPageToken' in data:
                next_page_token = data['nextPageToken']
            else:
                break
        
        print(len(video_ids))
        return video_ids
    
    except requests.exceptions.RequestException as e:
        raise e
    
if __name__ == "__main__":
    playlist_id = get_channel_playlist_id()
    get_video_ids(playlist_id)