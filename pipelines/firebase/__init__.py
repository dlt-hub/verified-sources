"""Loads data from firebase realtime database"""
import json

from typing import Iterator, List, Union

import dlt
from dlt.common.typing import DictStrAny, TDataItem
from dlt.extract.source import DltResource
from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials
from dlt.common.exceptions import MissingDependencyException

try:
    import firebase_admin
    from firebase_admin import credentials, db
except ImportError:
    raise MissingDependencyException("Firebase Admin Python SDK", ["firebase-admin"])

@dlt.source(name="firebase")
def firebase_source(
    firebase_credentials: Union[GcpOAuthCredentials, GcpServiceAccountCredentials] = dlt.secrets.value,
    database_url:  str = dlt.secrets.value,
    path: str = "/",
    app_name: str = "[DEFAULT]",
) -> List[DltResource]:
    
    def _get_data(credentials: DictStrAny, database_url: str) -> Iterator[TDataItem]:
        # choose already created app 
        firebase_admin.get_app(name=f"{app_name}")

        # load stored data
        ref = db.reference(f"{path}")
        data = json.loads(ref.get())

        yield data

    # build credentials
    app_credentials = credentials.Certificate(firebase_credentials.to_native_representation)

    @dlt.resource(write_disposition="replace")
    def realtime_db(
        database_url:  str = database_url,
        credentials: DictStrAny = app_credentials
    ) -> Iterator[TDataItem]:
        yield from _get_data(credentials, database_url)

    return [realtime_db]
