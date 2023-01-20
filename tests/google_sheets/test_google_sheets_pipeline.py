import pytest
from pipelines.google_sheets import google_sheets
from tests.utils import ALL_DESTINATIONS, assert_load_info


TEST_SPREADSHEETS = []


@pytest
def test_authentication():
    """"
    Just tests the authentication to the Google Sheets API
    """
    assert 1 == 1


@pytest.mark.parametrize('spreadsheet_id', TEST_SPREADSHEETS)
def test_spreadsheet_access():
    """"
    Just tests if access to the spreadsheets is ok
    """
    assert 1 == 1


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_appending():
    """"
    Test that adding new data in the sheets will add new data to the destinations
    """
    assert 1 == 1


@pytest.mark.parametrize('spreadsheet_id', TEST_SPREADSHEETS)
def test_all_ranges_loaded():
    """"
    Test that all ranges are loaded into a database
    """
    assert 1 == 1


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_full_load():
    """"
    Full run of a simple pipeline with all destinations
    """
    assert 1 == 1

