import json

from typing import Any, Dict, Generator, List, Union

import dlt
from dlt.common.typing import DictStrAny, StrAny, TDataItem
from dlt.extract.source import DltResource
from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials
from dlt.common.exceptions import MissingDependencyException

from firebase import firebase_source

try:
    import firebase_admin
    from firebase_admin import credentials, db
except ImportError:
    raise MissingDependencyException("Firebase Admin Python SDK", ["firebase-admin"])


_DATA =  {
    "discography": [
        {
            "band_name": "korn",
            "albums": [
                {
                    "album_name": "Requiem",
                    "year": 2022
                },
                {
                    "album_name": "The Nothing",
                    "year": 2019
                }
            ]
        },
        {
            "band_name": "slipknot",
            "albums": [
                {
                    "album_name": "The End, So Far",
                    "year": 2022
                },
                {
                    "album_name": "We Are Not Your Kind",
                    "year": 2019
                }
            ]
        }
    ]
}


@dlt.source(name="firebase_demo")
def firebase_source_demo(
    firebase_credentials: Union[GcpOAuthCredentials, GcpServiceAccountCredentials] = dlt.secrets.value,
    database_url: str = dlt.secrets.value,
    path: str = "/"
) -> List[DltResource]:

    def _get_data(credentials: DictStrAny, database_url: str) -> Generator[TDataItem, None, None]:
        # create an app and choose the database
        firebase_admin.initialize_app(
            credentials,
            {"databaseUrl": database_url}
        )
        # since we don't have data yet, let's add discography data into the db
        db.child("discography").set(db.push(_DATA))

        # load stored data
        ref = db.reference(f"{path}")
        data = json.loads(ref.get())

        yield data

    # build credentials
    app_credentials = credentials.Certificate(firebase_credentials.to_native_representation)

    @dlt.resource(write_disposition="replace")
    def realtime_db(
        database_url: str = database_url,
        credentials: DictStrAny = app_credentials
    ) -> Generator[TDataItem, None, None]:
        yield from _get_data(credentials, database_url)

    return [realtime_db]


def parse_data(data: Dict[Any, Any]) -> Generator[Dict[Any, Any], None, None]:
    # parse given JSON
    for item in data["discography"]:
        for album in item["albums"]:
            yield {
                "band_name": item["band_name"],
                "album_name": album["album_name"],
                "year": album["year"]
            }


def load_realtime_database(demo: bool = True) -> None:
    """loads realtime database pipeline"""
    pipeline = dlt.pipeline(
        pipeline_name="firebase_demo" if demo else "firebase",
        destination="duckdb",
        dataset_name="firebase_realtime_database",
        full_refresh=True
    )
    if demo:
        info = pipeline.run(firebase_source_demo().with_resources("realtime_db").add_map(parse_data))
    else:
        info = pipeline.run(firebase_source().with_resources("realtime_db"))
    print(info)


if __name__ == '__main__':
    # if you want to stream your own firebase data, change (demo=False)
    load_realtime_database(demo=True)
