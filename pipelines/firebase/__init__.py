"""Loads data from firebase realtime database"""

from typing import Any, Iterator, List, Sequence

import dlt
from dlt.extract.source import DltResource, DltSource
from dlt.common.typing import TDataItem

from firebase_admin import credentials, db


@dlt.source(name="firebase")
def firebase_source(
    project_id: dlt.secrets.value,
    private_key_id: dlt.secrets.value,
    private_key: dlt.secrets.value,
    client_email: dlt.secrets.value,
    client_id: dlt.secrets.value,
    database_url: dlt.secrets.value
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

        ref = db.reference("/")
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
    client_email_split = client_email.split("@")
    credentials_dict = {
        "type": "service_account",
        "project_id": project_id,
        "private_key_id": private_key_id,
        "private_key": private_key,
        "client_email": client_email,
        "client_id": client_id,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://accounts.google.com/o/oauth2/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email_split[0]}%40{client_email_split[1]}"
    }
    cred = credentials.Certificate(credentials_dict)

    @dlt.resource(write_disposition="replace")
    def realtime_db(
        database_url: database_url,
        credentials: Any = cred
    ) -> Iterator[TDataItem]:
        yield _get_data(database_url, credentials=credentials)
