"""Loads google play reviews, rating and more"""

from datetime import datetime
from typing import Any, Dict, Iterator, List, Sequence

import dlt
from dlt.extract.source import DltResource, DltSource
from dlt.common.typing import TDataItem

from googleapiclient.discovery import build

_SERVICE_NAME = "androidpublisher"
_SERVICE_VERSION = "v3"
"""Package name. Note: you must replace this with your own package name"""
package_name = "com.facebook.katana"

@dlt.source(name="google_play_reviews")
def google_play_reviews_source(
    api_secrets_key: str = dlt.secrets.value,
    package_name: str = package_name
) -> Sequence[DltResource]:

    def _get_review_and_rating(
            service: Any,
            package_name: str
    ):
        """
        Function to retrieve comments and rating

        params:
            service: Android publisher resource
            package_name: package name of your own app
        
        returns:
            review_and_rating: JSON contains review and rating
        """
        response = service.reviews().list(
            packageName=package_name
        ).execute()

        next_page_token = True
        while next_page_token:
            response = service.reviews().list(
                packageName=package_name,
                pageToken=next_page_token
            ).execute()
        
            for item in response["reviews"]:
                yield {
                    "review_id": item["reviewId"],
                    "comment": item["comments"]["userComment"]["text"],
                    "rating": item["comments"]["userComment"]["starRating"],
                    "device": item["comments"]["userComment"]["device"],
                    "android_version": item["comments"]["userComment"]["androidOsVersion"],
                    "app_version": item["comments"]["userComment"]["appVersionCode"],
                    "created_at": datetime.now()
                }

            next_page_token = response["tokenPagination"]["nextPageToken"]
    
    @dlt.resource(write_disposition="append")
    def review_and_rating(
        package_name: str,
        api_secrets_key: dlt.secrets.value
    ) -> Iterator[TDataItem]:
        
        # build resource
        service =  Any = build(_SERVICE_NAME, _SERVICE_VERSION,developerKey=api_secrets_key)
        
        yield _get_review_and_rating(
            service=service,
            package_name=package_name
        )
