import pytest
from pipelines.google_sheets import google_sheets
from tests.utils import ALL_DESTINATIONS, assert_load_info


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_load_url():
    """"

    """
    assert 1 == 1


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_load_id():
    """"

    """
    assert 1 == 1


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_load_ranges():
    """"

    """
    assert 1 == 1

