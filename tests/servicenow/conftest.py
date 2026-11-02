from typing import Iterator

import pytest
import requests_mock

from sources.servicenow.helpers.credentials import BasicCredentials

from tests.servicenow.utils import INSTANCE, FakeInstance


@pytest.fixture
def instance() -> Iterator[FakeInstance]:
    with requests_mock.Mocker() as m:
        yield FakeInstance(m)


@pytest.fixture
def credentials() -> BasicCredentials:
    # resolved credentials are not replaced with the ones from secrets.toml
    credentials = BasicCredentials(instance=INSTANCE, username="dlt", password="secret")
    credentials.resolve()
    return credentials
