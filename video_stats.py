import requests
import json
import os
from dotenv import load_dotenv
import itertools
from datetime import date

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_HANDLE = os.getenv("YOUTUBE_CHANNEL_HANDLE")
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
            
            if len(video_ids) % 1000 == 0:
                print(f"Fetched {len(video_ids)} videos so far...")

            if 'nextPageToken' in data:
                next_page_token = data['nextPageToken']
            else:
                break
        
        print(len(video_ids))
        return video_ids
    
    except requests.exceptions.RequestException as e:
        raise e
    
def get_video_stats(video_ids):
    try:
        extracted_data = []

        for batch in itertools.batched(video_ids, MAX_RESULTS_PER_PAGE):
            # Join the batch of 50 into a single comma-separated string for the API
            comma_separated_ids = ",".join(batch)

            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={comma_separated_ids}&key={API_KEY}"

            response = requests.get(url)
            response.raise_for_status()  # Check if the request was successful
            data = response.json()
            
            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]
            
                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None),
                }
                extracted_data.append(video_data)

            if(len(extracted_data) % 1000 == 0):
                print(f"Fetched {len(extracted_data)} video stats so far...")   
        
        print(len(extracted_data))
        return extracted_data
    
    except requests.exceptions.RequestException as e:
        raise e
    
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"

    try:
        with open(file_path, "w", encoding="utf-8") as json_outfile:
            json.dump(extracted_data, json_outfile, ensure_ascii=False, indent=4)
        print(f"Success! Data saved to: {file_path}")    
        
    except requests.exceptions.RequestException as e:
        print("Something went wrong with the network request!")
        raise e
    
if __name__ == "__main__":
    playlist_id = get_channel_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_stats = get_video_stats(video_ids)
    save_to_json(video_stats)