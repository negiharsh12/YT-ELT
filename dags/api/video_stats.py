import itertools
import json
import logging
from datetime import date

import requests
from airflow.decorators import task
from airflow.models import Variable

log = logging.getLogger(__name__)

API_KEY = Variable.get("YOUTUBE_API_KEY")
CHANNEL_HANDLE = Variable.get("YOUTUBE_CHANNEL_HANDLE")
MAX_RESULTS_PER_PAGE = 50
PROGRESS_EVERY = 100


def _progress(task_name: str, count: int, unit: str = "records") -> None:
    if count > 0 and count % PROGRESS_EVERY == 0:
        log.info("[%s] %s %s fetched so far", task_name, count, unit)


@task(show_return_value_in_logs=False)
def get_channel_playlist_id():
    log.info("[get_channel_playlist_id] started")

    if not API_KEY or not CHANNEL_HANDLE:
        raise ValueError(
            "Missing YOUTUBE_API_KEY or YOUTUBE_CHANNEL_HANDLE (check AIRFLOW_VAR_* / Variables)"
        )

    url = (
        "https://youtube.googleapis.com/youtube/v3/channels"
        f"?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
    )

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        log.info("[get_channel_playlist_id] YouTube API connected (HTTP %s)", response.status_code)

        data = response.json()
        items = data.get("items", [])
        if not items:
            raise ValueError(f"No channel found. API response: {data}")

        playlist_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
        log.info("[get_channel_playlist_id] completed (uploads playlist ready)")
        return playlist_id

    except requests.exceptions.RequestException:
        log.exception("[get_channel_playlist_id] failed")
        raise
    except (KeyError, IndexError):
        log.exception("[get_channel_playlist_id] unexpected API response shape")
        raise


@task(show_return_value_in_logs=False)
def get_video_ids(playlist_id):
    log.info("[get_video_ids] started")
    connected_logged = False

    try:
        video_ids = []
        next_page_token = ""

        while True:
            url = (
                "https://youtube.googleapis.com/youtube/v3/playlistItems"
                f"?part=contentDetails&maxResults={MAX_RESULTS_PER_PAGE}"
                f"&pageToken={next_page_token}&playlistId={playlist_id}&key={API_KEY}"
            )

            response = requests.get(url, timeout=30)
            response.raise_for_status()

            if not connected_logged:
                log.info("[get_video_ids] YouTube API connected (HTTP %s)", response.status_code)
                connected_logged = True

            data = response.json()
            for item in data.get("items", []):
                video_ids.append(item["contentDetails"]["videoId"])

            _progress("get_video_ids", len(video_ids), "video IDs")

            if "nextPageToken" in data:
                next_page_token = data["nextPageToken"]
            else:
                break

        log.info("[get_video_ids] completed (total video IDs: %s)", len(video_ids))
        return video_ids

    except requests.exceptions.RequestException:
        log.exception("[get_video_ids] failed")
        raise


@task(show_return_value_in_logs=False)
def get_video_stats(video_ids):
    log.info("[get_video_stats] started (input size: %s)", len(video_ids))
    connected_logged = False

    try:
        extracted_data = []

        for batch in itertools.batched(video_ids, MAX_RESULTS_PER_PAGE):
            comma_separated_ids = ",".join(batch)
            url = (
                "https://youtube.googleapis.com/youtube/v3/videos"
                f"?part=contentDetails&part=snippet&part=statistics"
                f"&id={comma_separated_ids}&key={API_KEY}"
            )

            response = requests.get(url, timeout=30)
            response.raise_for_status()

            if not connected_logged:
                log.info("[get_video_stats] YouTube API connected (HTTP %s)", response.status_code)
                connected_logged = True

            for item in response.json().get("items", []):
                snippet = item["snippet"]
                content_details = item["contentDetails"]
                statistics = item["statistics"]
                extracted_data.append(
                    {
                        "video_id": item["id"],
                        "title": snippet["title"],
                        "publishedAt": snippet["publishedAt"],
                        "duration": content_details["duration"],
                        "viewCount": statistics.get("viewCount"),
                        "likeCount": statistics.get("likeCount"),
                        "commentCount": statistics.get("commentCount"),
                    }
                )

            _progress("get_video_stats", len(extracted_data), "video stats")

        log.info("[get_video_stats] completed (total records: %s)", len(extracted_data))
        return extracted_data

    except requests.exceptions.RequestException:
        log.exception("[get_video_stats] failed")
        raise


@task(show_return_value_in_logs=False)
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"
    log.info("[save_to_json] started (records to write: %s)", len(extracted_data))

    try:
        with open(file_path, "w", encoding="utf-8") as json_outfile:
            json.dump(extracted_data, json_outfile, ensure_ascii=False, indent=4)
        log.info("[save_to_json] completed (wrote %s records)", len(extracted_data))
        return file_path

    except OSError:
        log.exception("[save_to_json] failed writing %s", file_path)
        raise


if __name__ == "__main__":
    playlist_id = get_channel_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_stats = get_video_stats(video_ids)
    save_to_json(video_stats)
