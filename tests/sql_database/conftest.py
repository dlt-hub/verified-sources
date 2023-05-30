from typing import Iterator
import pytest

import dlt
from dlt.sources.credentials import ConnectionStringCredentials

from tests.sql_database.sql_source import SQLAlchemySourceDB


@pytest.fixture(scope="package")
def sql_source_db(request: pytest.FixtureRequest) -> Iterator[SQLAlchemySourceDB]:
    # TODO: parametrize the fixture so it takes the credentials for all destinations
    credentials = dlt.secrets.get(
        "destination.postgres.credentials", expected_type=ConnectionStringCredentials
    )
    db = SQLAlchemySourceDB(credentials)
    db.create_schema()
    try:
        db.create_tables()
        db.insert_data()
        yield db
    finally:
        db.drop_schema()
