import datetime
from typing import Any, Dict, Iterator, List, Sequence

import dlt
from dlt.extract.source import DltResource
from dlt.common.typing import TDataItem

from googleapiclient.discovery import build


_SERVICE_NAME = "youtube"
_SERVICE_VERSION = "v3"

def _get_channel_id(youtube: Any, channel_name: str) -> str:
    """
    Function to retrieve channel id given channel custom name e.g. MrBeast

    params:
        youtube: YouTube resource object
        channel_name: YouTube custom channel name

    return:
        channel_id: YouTube channel id
    """
    try:
         response = youtube.search().list(
              part="snippet",
              q=f"{channel_name}",
              type="channle"
         ).execute
    except HttpError as e:
        if e.resp.status == 400:
            print("Bad request. Invalid channel name or parameter.")
            return
        else:
             print(" An HTTP error occured: ", e)
             return 

    # retrive the channel id and handle if null
    if "items" in response:
        for item in response["items"]:
            #filter only youtube channel
            if item["id"]["kind"] == "youtube#channel":
                return item["id"]["channelId"]
    else:
        print(f"{channel_name} was not found, make sure you input the correct channel name")
        return

def _get_channel_video(
        youtube: Any, 
        channel_id: str,
        start_date: str,
        end_date: str,
        max_results: int = 50
) -> List[str]:
    """
    Function to retrieve all published videos given channel id

    params:
        youtube: YouTube resource object
        channel_id: YouTube channel id
        max_results: max results to be displayed each API call
        start_date: start date of published video
        end_date: last date of published video

    return:
        video_ids: list of videos ids
    """
    video_ids = []
    next_page_token = True

    while next_page_token:
        response = youtube.search().list(
            channelId=channel_id,
            part="id",
            maxResults=max_results,
            publishedAfter=start_date,
            publishedBefore=end_date,
            page_token=next_page_token
        ).execute()

        for item in response['items']:
            if 'videoId' in item['id']:
                video_id = item['id']['videoId']
                video_ids.append(video_id)
        
        # check if there are next page results
        if 'nextPageToken' not in response:
            next_page_token = False

    return video_ids

def _get_video_details(youtube: Any, video_list: List[str], max_results: int) -> List[Dict[str, str]]:
    """
    Function to retrieve detail information given an video id

    params:
        youtube: YouTube resource object
        video_list: list of Youtube video id
        max_results: How many results displayed each API call

    return:
        stats_list: Dict. that contains detail information of the videos
    """
    stats_list = []

    # can only get as max_results at a time
    for i in range(0, len(video_list), max_results):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_list[i:i+max_results]
        ).execute()

        for video in response["items"]:
            title = video["snippet"]["title"]
            published = video["snippet"]["publishedAt"]
            description = video["snippet"]["description"]
            tags = video["snippet"].get("tags", [])
            tags_count = len(tags) if tags else 0
            views_count = video["statistics"].get("viewCount", 0)
            likes_count = video["statistics"].get("likeCount", 0)
            dislikes_count = video["statistics"].get("dislikeCount", 0)
            comments_count = video["statistics"].get("commentCount", 0)
            stats_dict = dict(
                title=title,
                published=published, 
                description=description,
                tags=tags,
                tags_count=tags_count,
                views_count=views_count,
                likes_count=likes_count,
                dislikes_count=dislikes_count,
                comments_count=comments_count
            )
            stats_list.append(stats_dict)

    return stats_list

@dlt.source(name="youtube_data")
def youtube_data_source(
    channel_names: List[str],
    start_date: str,
    end_date: str,
    max_results: int
) -> Sequence[DltResource]:
    
    # youtube API using YYYY-MM-DDTHH:MM:SSZ format
    start_date = start_date+'T00:00:00Z'
    end_date = end_date+'T00:00:00Z'
    max_results = 50

    return youtube_data(
        channel_names,
        start_date,
        end_date,
        max_results
    )

@dlt.resource(write_disposition="replace")
def youtube_data(
    channel_names: List[str],
    start_date: str,
    end_date: str,
    max_results: int,
    youtube: Any = build(_SERVICE_NAME, _SERVICE_VERSION, developerKey=dlt.secrets.value),
) -> Iterator[TDataItem]:
    
    for channel_name in channel_names:
        # get channel_id given channel_name
        channel_id = _get_channel_id(youtube, channel_name)

        # handle if channel_id return null (channel id not found)
        if not channel_id:
            continue

        # get channel video ids
        video_ids = _get_channel_video(youtube, channel_id, start_date, end_date, max_results)

        # get videos statistic
        video_details = _get_video_details(youtube, video_ids, max_results)

        for item in video_details:
            yield {
                "channel_id": channel_id,
                "channel_name": channel_name,
                "title": item["title"],
                "published": item["published"],
                "description": item["description"],
                "tags": item["tags"],
                "tags_count": item["tags_count"],
                "views_count": item["views_count"],
                "likes_count": item["likes_count"],
                "dislikes_count": item["dislikes_count"],
                "comments_count": item["comments_count"],
                "created_at": datetime.datetime.now()
            }
