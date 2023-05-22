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
    path: str = "/",
    app_name: str = "[DEFAULT]"
) -> Sequence[DltResource]:
    
    def _get_data(credentials: Any, database_url: str) -> Iterator[TDataItem]:
        try:
            # if there is no app created yet, create an app and choose the database
            firebase_admin.initialize_app(
                credentials, 
                {"databaseUrl": database_url}
            )
        except ValueError:
            # choose already created app 
            firebase_admin.get_app(name=f"{app_name}")

        ref = db.reference(f"{path}")
        data = json.loads(ref.get())

        yield data

    # build credentials
    app_credentials = credentials.Certificate(firebase_credentials.to_native_representation)

    @dlt.resource(write_disposition="replace")
    def realtime_db(
        database_url: database_url,
        credentials: Any = app_credentials
    ) -> Iterator[TDataItem]:
        yield _get_data(database_url)
