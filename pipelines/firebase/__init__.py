"""Loads data from firebase realtime database"""

from typing import Any, Iterator, Sequence, Union

import duckdb
import pandas as pd

import dlt
from dlt.extract.source import DltResource
from dlt.common.typing import TDataItem
from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials
from dlt.common.exceptions import MissingDependencyException

try:
    from firebase_admin import credentials, db
except ImportError:
    raise MissingDependencyException("Firebase Admin Python SDK", ["firebase-admin"])

try:
    from apiclient.discovery import Resource
except ImportError:
    raise MissingDependencyException("Google API Client", ["google-api-python-client"])


@dlt.source(name="firebase")
def firebase_source(
    firebase_credentials: Union[GcpOAuthCredentials, GcpServiceAccountCredentials] = dlt.secrets.value,
    database_url:  str = dlt.secrets.value,
    path: str = "/"
) -> Sequence[DltResource]:
    
    def _get_data(credentials: Any, database_url: str) -> Iterator[TDataItem]:
        """
        Note: For this pipeline, we use a mock json template such below,
        your firebase realtime data likely different:
        {
            "discography": [
                {
                    "albums": [
                        {
                            "album_name": str,
                            "year": int
                        }
                    ],
                    "band_name": str
                }
            ]
        }
        """

        # initialize app
        try:
            firebase_admin.initialize_app(
                credentials, 
                {"databaseUrl": database_url}
            )
        except ValueError:
            firebase_admin.get_app(name="[DEFAULT]")

        ref = db.reference(f"{path}")
        data = json.loads(ref.get())

        """Please adjust the loop part according to your firebase database json schema"""
        for item in data["discography"]:
            for album in item["albums"]:
                yield {
                    "band_name": item["band_name"],
                    "album": album["album_name"],
                    "year": album["year"]
                }

    # build credentials
    credentials = credentials.Certificate(firebase_credentials.to_native_representation)

    @dlt.resource(write_disposition="replace")
    def realtime_db(
        database_url: database_url,
        credentials: Any = cred
    ) -> Iterator[TDataItem]:
        yield _get_data(database_url, credentials=credentials)
